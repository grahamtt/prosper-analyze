from dask import dataframe as df
import logging
from os import path

logger = logging.getLogger(__file__)

logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s", level=logging.INFO
)

try:
    logger.info("Loading listings...")
    listing_ds = df.read_csv(path.join(path.dirname(__file__), "..", "data", "listings.csv"),encoding='latin-1',assume_missing=True) #, index_col=['loan_origination_date','amount_funded','borrower_rate','listing_term'],header=0)
    # listing_ds = all_listing_ds.query("loan_origination_date > '2020'")
    logger.info("Creating composite index column for listings...")
    listing_ds["index"] = listing_ds['loan_origination_date'].astype(str) + "|" + listing_ds['borrower_rate'].astype(str) + "|" + listing_ds['listing_term'].astype(str) + "|" + listing_ds['amount_funded'].astype(str)
    listing_ds.compute()
    logger.info("Sorting listings by composite index...")
    listing_ds = listing_ds.set_index(other="index", drop=True)
    # logger.info(f"Using the latest {len(listing_ds)} loans out of {len(all_listing_ds)}")
    logger.info("Loading loans...")
    loan_ds = df.read_csv(path.join(path.dirname(__file__), "..", "data", "loans.csv"),encoding='latin-1',assume_missing=True) #, index_col=['origination_date','amount_borrowed','borrower_rate','term'], header=0)
    logger.info("Creating composite index column for loans...")
    loan_ds["index"] = loan_ds['origination_date'].astype(str) + "|" + loan_ds['borrower_rate'].astype(str) + "|" + loan_ds['term'].astype(str) + "|" + loan_ds['amount_borrowed'].astype(str)
    loan_ds.compute()
    logger.info("Sorting loans by composite index...")
    loan_ds = loan_ds.set_index("index", drop=True)

    # counts = {"duplicate":0, "no_match": 0, "match":0}
    # def results_generator():
    #     i = 0
    #     for [index, listing_row] in listing_ds.iterrows():
    #         # logger.info(f"Matching listing {listing_row}")
    #         query_str = f"origination_date == '{listing_row['loan_origination_date']}' & amount_borrowed == '{listing_row['amount_funded']}' & borrower_rate == {listing_row['borrower_rate']} & term == {listing_row['listing_term']}"
    #         match_ds = loan_ds.query(query_str)
            
    #         if len(match_ds):
    #             if len(match_ds > 1):
    #                 logger.debug(f"{len(match_ds)} matches found for {query_str}")
    #                 counts['duplicate']+=1
    #             counts['match']+=1
    #             loan_row = match_ds.iloc[0]
    #             total_paid = loan_row['principal_paid'] + loan_row['interest_paid']
    #             raw_return_rate = (total_paid - loan_row['amount_borrowed']) / loan_row['amount_borrowed']
    #             scaled_return_rate = raw_return_rate / loan_row['term']
    #             yield [listing_row['listing_number'], [True, total_paid, raw_return_rate, scaled_return_rate]]
    #         else:
    #             counts['no_match'] += 1
    #             yield [listing_row['listing_number'], [False, 0, 0, 0]]

    #         if i % 1000 == 0:
    #             logger.info(f"Matched {i+1} listings")
    #         i += 1

    # def index_generator():
    #     for [index, listing_row] in listing_ds.iterrows():
    #         yield index

    # index = [int(row['listing_number']) if hasattr(row, 'listing_number') else 0 for row in listing_ds.iterrows()]
    columns = ['loan_funded', 'total_paid', 'total_return_rate', 'scaled_return_rate']
    # results = DataFrame(results_generator(),
    #                     index=index_generator(), 
    #                     columns=columns)
    logger.info("Joining datasets...")
    joined_ds = listing_ds.join(loan_ds, rsuffix="_loan", ) #, on=['loan_origination_date', 'amount_funded', 'borrower_rate', 'listing_term'])
    joined_ds['total_return'] = joined_ds['principal_paid'].fillna(0) + joined_ds['interest_paid'].fillna(0) + joined_ds['late_fees_paid'].fillna(0) + joined_ds['debt_sale_proceeds_received'].fillna(0)
    joined_ds['raw_return_rate'] = (joined_ds['total_return'] - joined_ds['amount_borrowed'].fillna(0))/joined_ds['amount_borrowed'].fillna(1)
    joined_ds['scaled_return_rate'] = (joined_ds['raw_return_rate'] * 12) / joined_ds['listing_term']

    # Having to drop duplicates means the join needs work. The results will be bad
    final_column_names = listing_ds.columns.tolist()
    final_column_names.append('scaled_return_rate')
    results = joined_ds[final_column_names]\
        .drop_duplicates(subset=['listing_number'])\
        .set_index('listing_number', drop=True)
    logger.info(f"Dropped {len(joined_ds) - len(results)} duplicate :(")

    df.to_csv(results, path.join(path.dirname(__file__), "..", "data", "results.csv"), single_file=True)
    

    # if counts['duplicate'] > 0:
    #     logger.warning(f"{counts['duplicate']} duplicate matches detected")

    # logger.info(f"{counts['match']} matched listings found")
    # logger.info(f"{counts['no_match']} unmatched listings found")
    # logger.info(f"{len(results.query('loan_funded'))} loans funded out of {len(results)}")

    pass
except Exception as e:
    logger.error(e)
    raise e