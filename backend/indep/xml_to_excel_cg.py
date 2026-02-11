import pandas as pd
import xml.etree.ElementTree as ET
import sys
from collections import defaultdict
import re


def parse_cg_xml_to_excel(xml_file, output_excel):
    """
    Parses a Corporate Governance XBRL file and extracts its structured data
    into multiple sheets in a single Excel file.
    """
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()
    except ET.ParseError as e:
        print(f"Error parsing XML file {xml_file}: {e}")
        return

    # --- Step 1: Extract all facts into a flat list ---
    facts = []
    for elem in root.iter():
        if elem.text and elem.text.strip():
            # Clean up the element name by removing the namespace prefix
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
        print("No facts with context references found.")
        return

    # --- Step 2: Prepare dictionaries to hold data for different tables ---
    general_info = {}
    board_composition = defaultdict(dict)
    committee_composition = defaultdict(dict)
    board_meetings = defaultdict(dict)
    committee_meetings = defaultdict(dict)

    # Regex to find the numeric ID at the end of the context string
    id_pattern = re.compile(r'(\d+)$')

    # --- Step 3: Categorize and group facts ---
    for fact in facts:
        context = fact['Context']
        element_name = fact['Element Name']
        value = fact['Fact Value']
        match = id_pattern.search(context)

        # General Info (facts with context 'MainD' or 'MainI')
        if context in ['MainD', 'MainI']:
            general_info[element_name] = value
            continue

        if not match:
            continue

        item_id = int(match.group(1))

        # Categorize based on the context name prefix
        if 'CompBOD' in context:
            board_composition[item_id][element_name] = value
        elif 'CompComit' in context:
            committee_composition[item_id][element_name] = value
        elif 'MeetingBOD' in context:
            board_meetings[item_id][element_name] = value
        elif 'MeetingComit' in context:
            committee_meetings[item_id][element_name] = value

    # --- Step 4: Create DataFrames and write to Excel sheets ---
    with pd.ExcelWriter(output_excel, engine='openpyxl') as writer:
        print("Writing 'General_Info' sheet...")
        if general_info:
            pd.DataFrame([general_info]).to_excel(writer, sheet_name='General_Info', index=False)

        print("Writing 'Board_Composition' sheet...")
        if board_composition:
            pd.DataFrame(board_composition.values()).to_excel(writer, sheet_name='Board_Composition', index=False)

        print("Writing 'Committee_Composition' sheet...")
        if committee_composition:
            pd.DataFrame(committee_composition.values()).to_excel(writer, sheet_name='Committee_Composition',
                                                                  index=False)

        print("Writing 'Board_Meetings' sheet...")
        if board_meetings:
            pd.DataFrame(board_meetings.values()).to_excel(writer, sheet_name='Board_Meetings', index=False)

        print("Writing 'Committee_Meetings' sheet...")
        if committee_meetings:
            pd.DataFrame(committee_meetings.values()).to_excel(writer, sheet_name='Committee_Meetings', index=False)

    print(f"\nSuccessfully created Excel file at: {output_excel}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python cg_xml_to_excel.py <input_xml_file> <output_excel_file>")
        sys.exit(1)

    input_xml = sys.argv[1]
    output_excel = sys.argv[2]

    parse_cg_xml_to_excel(input_xml, output_excel)
