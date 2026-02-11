import os
import sys
import xml.etree.ElementTree as ET
import pandas as pd
from sqlalchemy import create_engine, inspect
from dotenv import load_dotenv
import re
from collections import defaultdict


def get_db_columns(engine, table_name, schema='public'):
    """
    Gets the list of column names from a specified table in the database.
    """
    try:
        inspector = inspect(engine)
        columns = [col['name'] for col in inspector.get_columns(table_name, schema=schema)]
        return columns
    except Exception as e:
        print(f"Error fetching columns for table {schema}.{table_name}: {e}")
        return None


def parse_xbrl_to_grouped_df(xml_file):
    """
    Parses an XBRL file and groups related facts into a structured DataFrame.
    Each row in the output DataFrame represents a single related party transaction.
    """
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()
    except ET.ParseError as e:
        print(f"Error parsing XML file {xml_file}: {e}")
        return pd.DataFrame()

    facts = []
    for elem in root.iter():
        if elem.text and elem.text.strip():
            element_name = elem.tag.split('}')[-1]
            context_ref = elem.get('contextRef', '')
            fact_value = elem.text.strip()

            if context_ref:
                facts.append({
                    'Element Name': element_name,
                    'Context': context_ref,
                    'Fact Value': fact_value
                })

    if not facts:
        return pd.DataFrame()

    grouped_facts = defaultdict(dict)
    id_pattern = re.compile(r'(\d+)$')

    for fact in facts:
        context = fact['Context']
        match = id_pattern.search(context)

        if match:
            transaction_id = int(match.group(1))
            element_name = fact['Element Name']
            value = fact['Fact Value']

            if context.startswith('D_'):
                grouped_facts[transaction_id][element_name] = value
            elif context.startswith('RelatedPartyTransaction_PY'):
                grouped_facts[transaction_id]['AmountOfRelatedPartyTransaction_PreviousYear'] = value
            else:
                grouped_facts[transaction_id]['AmountOfRelatedPartyTransaction_Outstanding'] = value

    transaction_list = []
    company_name = root.findtext('.//{*}NameOfTheCompany', default='Unknown')
    scrip_code = root.findtext('.//{*}ScripCode', default='Unknown')

    for transaction_id, details in grouped_facts.items():
        details['TransactionID'] = transaction_id
        details['CompanyName'] = company_name
        details['ScripCode'] = scrip_code
        transaction_list.append(details)

    if not transaction_list:
        return pd.DataFrame()

    df = pd.DataFrame(transaction_list)

    # --- NEW CODE: Rename DataFrame columns to match the new DB schema ---
    rename_mapping = {
        "InterestRateOfLoansOrInterCorporateDepositsOrAdvancesOrInvestments": "IROfLoansOrInterCorporateDepositsOrAdvancesOrInvestments",
        "TypeOfOfLoansOrInterCorporateDepositsOrAdvancesOrInvestmentsSecuredOrUnsecured": "TypeOfOfLoansOrICDOrAdvancesOrInvestmentsSecuredOrUnsecured",
        "PurposeForWhichTheFundsWillBeUtilisedByTheUltimateRecipientOfFundsForEndusage": "PurposeOfUtilisationOfTheUltimateRecipientOfFundsForEndusage"
    }
    df.rename(columns=rename_mapping, inplace=True)
    # --- END OF NEW CODE ---

    numeric_cols = [
        "AmountOfRelatedPartyTransactionDuringTheReportingPeriod",
        "AmountOfRelatedPartyTransaction_Outstanding",
        "AmountOfRelatedPartyTransaction_PreviousYear",
        "ValueOfTheRelatedPartyTransactionAsApprovedByTheAuditCommittee"
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    return df


if __name__ == "__main__":
    load_dotenv()

    db_url = os.getenv("POSTGRES_URL")
    if not db_url:
        print("POSTGRES_URL not found in .env file")
        sys.exit(1)

    try:
        engine = create_engine(db_url)
        db_columns = get_db_columns(engine, 'rpt', schema='public')
        if not db_columns:
            print("Could not retrieve columns from the database. Exiting.")
            sys.exit(1)
        print(f"Target database columns found: {db_columns}")
    except Exception as e:
        print(f"Failed to connect to the database: {e}")
        sys.exit(1)

    script_dir = os.path.dirname(os.path.abspath(__file__))
    rpt_folder = os.path.abspath(os.path.join(script_dir, '..', 'rpt'))

    if not os.path.isdir(rpt_folder):
        print(f"Directory not found: {rpt_folder}")
        sys.exit(1)

    for filename in os.listdir(rpt_folder):
        if filename.endswith(".xml") and ':Zone.Identifier' not in filename:
            file_path = os.path.join(rpt_folder, filename)
            print(f"\nProcessing {file_path}...")
            df = parse_xbrl_to_grouped_df(file_path)

            if not df.empty:
                cols_to_keep = [col for col in df.columns if col in db_columns]
                df_filtered = df[cols_to_keep]

                missing_cols = [col for col in df.columns if col not in db_columns]
                if missing_cols:
                    print(f"  INFO: Ignoring columns not in DB: {missing_cols}")

                try:
                    df_filtered.to_sql('rpt', engine, schema='public', if_exists='append', index=False)
                    print(f"  SUCCESS: Inserted {len(df_filtered)} records from {filename} into the database.")
                except Exception as e:
                    print(f"  ERROR: Could not insert data from {filename}. Reason: {e}")
            else:
                print(f"  INFO: No data to insert for {filename}")
