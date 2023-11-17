# -*- coding: utf-8 -*-
from argparse import ArgumentParser

import etherscan as eth
import pandas as pd
import numpy as np
from cassandra.cluster import Cluster
from random import sample, choice
from time import sleep

import logging
logging.basicConfig(format="%(asctime)s %(levelname)-8s %(message)s", level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S")
logging.getLogger("cassandra").setLevel(logging.CRITICAL)
logging.getLogger("Cluster").setLevel(logging.CRITICAL)


class EthValidate(object):
    def __init__(self, apikey, cluster_addresses, keyspace):
        self.etherscan = eth.Etherscan(apikey)
        self.cluster_nodes = cluster_addresses
        self.keyspace = keyspace
        self.apikey = apikey

    def _get_from_es(self, adr: str, sb, eb, sort, offset=5000, blockstep=500_000):
        res = []

        fromblock = sb

        while fromblock < eb:
            toblock = min(fromblock+blockstep, eb)
            page = 1

            try:
                while True:
                    res.extend(self.etherscan.get_normal_txs_by_address_paginated(adr, page=page, startblock=fromblock, endblock=toblock, offset=offset, sort=sort))
                    page += 1
                    sleep(0.1)
            except AssertionError as e:  # thrown by etherscan
                if e.__str__().startswith("None"):
                    raise ValueError(f"API threw error '{e}'. Please fix offset and blockstep size.")

                pass  # etherscan api did not find any more results, which is fine

            fromblock += blockstep

        logging.info(f"Collected {len(res)} tx")
        return res

    def get_normal_tx_rom_etherscan_api(self, adr: str, endblock: int, maxlength=10_000) -> pd.DataFrame:
        r = self.etherscan.get_normal_txs_by_address(adr, 0, endblock, "asc")

        if len(r) >= maxlength:
            logging.info(f"API returned its limit of {maxlength} transactions. Trying to retrieve _all_ tx now; this will take a while (and possible fail).")
            startblock = r[0]["blockNumber"]
            r = self._get_from_es(adr, int(startblock), int(endblock), "asc")

        for x in r:
            x["value"] = int(x["value"])

        # set dtype=object to avoid problems with huge wei amounts and numeric datatypes later on
        df = pd.DataFrame.from_dict(r, dtype=object)
        df = df.astype(dtype={"blockNumber": int, "timeStamp": int, "transactionIndex": int})

        # remove contract transactions
        df.drop(df[df.contractAddress != ""].index, inplace=True)

        return df

    def _prepare_etherscan_reference(self, address, endblock) -> dict:
        result = dict()

        ref_df = self.get_normal_tx_rom_etherscan_api(address, endblock)

        ref_df["txtype"] = np.where(ref_df["from"] == address, "OUT", "IN")
        ref_df.loc[ref_df["from"] == ref_df["to"], "txtype"] = "INOUT"

        fb, fts = ref_df[ref_df["blockNumber"] == ref_df["blockNumber"].min()][["blockNumber", "timeStamp"]].iloc[0]
        lb, lts = ref_df[ref_df["blockNumber"] == ref_df["blockNumber"].max()][["blockNumber", "timeStamp"]].iloc[0]
        result["first_block"] = int(fb)
        result["first_timestamp"] = int(fts)
        result["last_block"] = int(lb)
        result["last_timestamp"] = int(lts)

        no_tx = ref_df.groupby("txtype").nunique()["hash"].reset_index().set_index("txtype")
        in_tx = no_tx.at["IN", "hash"] if "IN" in no_tx.index else 0
        out_tx = no_tx.at["OUT", "hash"] if "OUT" in no_tx.index else 0

        # tx to self count as separate transactions in Graphsense
        tx_to_self = no_tx.at["INOUT", "hash"] if "INOUT" in no_tx.index else 0
        degree = 1 if tx_to_self > 0 else 0

        # add tx to self if any
        in_tx += tx_to_self
        out_tx += tx_to_self

        result["in_tx"] = in_tx
        result["out_tx"] = out_tx

        # in and out degree, taking into account tx to self
        in_degree = ref_df[ref_df["from"] != address]["from"].nunique() + degree
        out_degree = ref_df[ref_df["to"] != address]["to"].nunique() + degree
        result["in_degree"] = in_degree
        result["out_degree"] = out_degree

        sums = ref_df[["txtype", "value"]].groupby("txtype").sum().reset_index().set_index("txtype")

        in_wei = sums.loc["IN", "value"] if "IN" in sums.index else 0
        out_wei = sums.loc["OUT", "value"] if "OUT" in sums.index else 0
        inout_wei = sums.loc["INOUT", "value"] if "INOUT" in sums.index else 0

        result["in_wei"] = in_wei + inout_wei
        result["out_wei"] = out_wei + inout_wei
        return result

    def _prepare_graphsense_data(self, session, address_id_group, address_id) -> dict:
        # TODO: get data from API when it's finished
        result = session.execute(f"SELECT * FROM {self.keyspace}.address WHERE address_id_group = {address_id_group} AND address_id = {address_id}")

        r = result.current_rows[0]
        d = dict()

        first_block, ftid, fts = r.first_tx
        last_block, ltid, lts = r.last_tx

        d["first_block"] = first_block
        d["first_timestamp"] = fts
        d["last_block"] = last_block
        d["last_timestamp"] = lts

        in_wei, in_fiat_list = r.total_received
        out_wei, out_fiat_list = r.total_spent

        d["in_wei"] = in_wei
        d["out_wei"] = out_wei

        d["in_tx"] = r.no_incoming_txs
        d["in_degree"] = r.in_degree
        d["out_tx"] = r.no_outgoing_txs
        d["out_degree"] = r.out_degree

        return d

    def _get_sample_addresses(self, sample_size, session):
        g = session.execute(f"SELECT address_id_group FROM {self.keyspace}.address_ids_by_address_id_group PER PARTITION LIMIT 1")
        groups = [row.address_id_group for row in g.current_rows]

        if len(groups) < 1:
            return []

        selected_groups = sample(groups, sample_size)

        selected_input = []
        for group in selected_groups:
            address_ranges = session.execute(f"SELECT address_id_group, min(address_id) AS min_adr, max(address_id) AS max_adr FROM {self.keyspace}.address_ids_by_address_id_group WHERE address_id_group = {group}")
            row = address_ranges.current_rows[0]
            a = choice(range(row.min_adr, row.max_adr))
            selected_input.append((row.address_id_group, a))

        selected_addresses = []
        for (address_group, address_id) in selected_input:
            res = session.execute(f"SELECT * FROM {self.keyspace}.address_ids_by_address_id_group WHERE address_id_group = {address_group} AND address_id={address_id}")
            d = res.current_rows[0]
            hex_string = ''.join('{:02x}'.format(x) for x in d.address)
            selected_addresses.append((d.address_id_group, d.address_id, f"0x{hex_string}"))

        return selected_addresses

    def _prepare(self, address, session, batchsize=50_000):
        prefix = address[2:6].upper()
        res = session.execute(f"SELECT address_id from {self.keyspace}.address_ids_by_address_prefix WHERE address_prefix = '{prefix}' AND address={address.upper()} ")

        if len(res.current_rows) == 0:
            raise ValueError(f"{address} not found in database")

        gs_id = res.current_rows[0][0]
        gs_group = int(gs_id/batchsize)

        return [(gs_group, gs_id, address)]

    def _compare(self, ref, act):
        errors = []

        for key in set(ref.keys()):
            if not ref[key] == act[key]:
                errors.append((key, "ref", ref[key], "act", act[key]))
        return errors

    def validate(self, address, samples=10):
        cluster = Cluster(self.cluster_nodes)
        session = cluster.connect(self.keyspace)

        if address:
            address = address.lower()
            random_addresses = self._prepare(address, session)
        else:
            logging.info(f"selecting {samples} random addresses from database")
            random_addresses = self._get_sample_addresses(samples, session)

        logging.info(f"validating {len(random_addresses)} addresses")

        for x in random_addresses:
            logging.info(x)
            group, gs_id, address = x

            actual = self._prepare_graphsense_data(session, group, gs_id)

            last_block = actual["last_block"]

            reference = self._prepare_etherscan_reference(address, last_block)

            match = self._compare(reference, actual)

            if len(match) > 0:
                print(f"{address}: Etherscan does not match Graphsense\n{group} {gs_id}\nerrors {match}")

        cluster.shutdown()


def main():
    parser = ArgumentParser(description="Compare Graphsense addresses against etherscan.io", epilog="GraphSense - http://graphsense.info")

    parser.add_argument("-d", "--db_nodes", dest="db_nodes", required=True, nargs="+", metavar="DB_NODE", help="list of Cassandra nodes")
    parser.add_argument("-k", "--keyspace", dest="keyspace", default="eth_transformed", metavar="ETH_TRANSFORMED", help="Cassandra keyspace to use")
    parser.add_argument("-a", "--apikey", dest="apikey", metavar="API_KEY", help="etherscan.io apikey")
    parser.add_argument("-s", "--samplesize", dest="samplesize", default="3", type=int, metavar="SAMPLE_SIZE", help="number of random addresses to be compared")
    parser.add_argument("-e", "--address", dest="address", metavar="0xFA8E3920daF271daB92Be9B87d9998DDd94FEF08", help="a specific Ethereum address to validate")

    args = parser.parse_args()

    etherscan_comparer = EthValidate(args.apikey, cluster_addresses=args.db_nodes, keyspace=args.keyspace)

    etherscan_comparer.validate(args.address, args.samplesize)


if __name__ == "__main__":
    main()
