import pandas as pd
import xml.etree.ElementTree as ET
import sys
from collections import defaultdict
import re


def parse_xbrl_to_grouped_df(xml_file):
    """
    Parses an XBRL file and groups related facts into a structured DataFrame.
    Each row in the output DataFrame represents a single related party transaction.
    """
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()
    except ET.ParseError as e:
        print(f"Error parsing XML file: {e}")
        return pd.DataFrame()  # Return empty DataFrame on error

    # --- Step 1: Extract all facts into a flat list ---
    facts = []
    for elem in root.iter():
        if elem.text and elem.text.strip():
            element_name = elem.tag.split('}')[-1]
            context_ref = elem.get('contextRef', '')
            fact_value = elem.text.strip()

            # We only care about facts that have a context reference
            if context_ref:
                facts.append({
                    'Element Name': element_name,
                    'Context': context_ref,
                    'Fact Value': fact_value
                })

    if not facts:
        print("No facts with context references found in the file.")
        return pd.DataFrame()

    # --- Step 2: Group facts by transaction ID ---
    # The defaultdict allows us to easily append to a list for a new key
    grouped_facts = defaultdict(dict)

    # Regex to find the numeric ID at the end of the context string
    # e.g., "D_RelatedPartyTransaction12" -> "12"
    id_pattern = re.compile(r'(\d+)$')

    for fact in facts:
        context = fact['Context']
        match = id_pattern.search(context)

        if match:
            transaction_id = int(match.group(1))
            element_name = fact['Element Name']
            value = fact['Fact Value']

            # Distinguish between current, previous year, and outstanding balances
            if context.startswith('D_'):
                # This is the main data for the current period
                grouped_facts[transaction_id][element_name] = value
            elif context.startswith('RelatedPartyTransaction_PY'):
                # Previous Year's Amount
                grouped_facts[transaction_id]['AmountOfRelatedPartyTransaction_PreviousYear'] = value
            else:
                # Outstanding Balance at period end
                grouped_facts[transaction_id]['AmountOfRelatedPartyTransaction_Outstanding'] = value

    # --- Step 3: Convert the grouped dictionary to a list of records ---
    transaction_list = []
    for transaction_id, details in grouped_facts.items():
        details['TransactionID'] = transaction_id
        transaction_list.append(details)

    if not transaction_list:
        print("Could not find any grouped related party transactions.")
        return pd.DataFrame()

    # --- Step 4: Create a clean DataFrame ---
    df = pd.DataFrame(transaction_list)

    # Reorder columns to be more logical
    # We'll define a preferred order and then add any other columns that might appear
    preferred_columns = [
        'TransactionID',
        'NameOfCounterParty',
        'RelationshipOfTheCounterpartyWithTheListedEntityOrItsSubsidiary',
        'TypeOfRelatedPartyTransaction',
        'AmountOfRelatedPartyTransactionDuringTheReportingPeriod',
        'AmountOfRelatedPartyTransaction_Outstanding',
        'AmountOfRelatedPartyTransaction_PreviousYear',
        'ValueOfTheRelatedPartyTransactionAsApprovedByTheAuditCommittee',
        'DetailsOfOtherRelatedPartyTransaction',
        'RemarksOnApprovalByAuditCommittee',
        'NameOfListedEntityOrSubsidiaryEnteringIntoTheTransaction'
    ]

    # Get existing columns from the DataFrame
    existing_columns = df.columns.tolist()
    # Create the final column order
    final_columns = [col for col in preferred_columns if col in existing_columns]
    # Add any remaining columns that weren't in our preferred list
    other_columns = [col for col in existing_columns if col not in preferred_columns]
    final_columns.extend(other_columns)

    df = df[final_columns]

    # Sort by the transaction ID to maintain the original order
    df = df.sort_values(by='TransactionID').reset_index(drop=True)

    return df


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python advanced_xbrl_parser.py <input_xml> <output_excel>")
        sys.exit(1)

    input_xml = sys.argv[1]
    output_excel = sys.argv[2]

    print(f"Processing {input_xml}...")
    df = parse_xbrl_to_grouped_df(input_xml)

    if not df.empty:
        # Save the structured data to Excel
        df.to_excel(output_excel, index=False, sheet_name='Related Party Transactions')
        print(f"Conversion complete. Structured Excel saved to {output_excel}")
    else:
        print("Failed to generate a report as no structured data was found.")
