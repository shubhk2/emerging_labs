import pandas as pd
import xml.etree.ElementTree as ET
import sys
from collections import defaultdict
import re


def parse_brsr_xml_to_excel(xml_file, output_excel):
    """
    Parses a BRSR XML file and extracts its structured data into multiple sheets
    in a single Excel file for analysis.
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

    # --- Step 2: Prepare dictionaries for different data tables ---
    general_info = {}
    holdings_subsidiaries = defaultdict(dict)
    csr_projects = defaultdict(dict)
    employee_details = defaultdict(lambda: defaultdict(dict))

    id_pattern = re.compile(r'(\d+)$')

    # --- Step 3: Categorize and group facts (Corrected Logic) ---
    for fact in facts:
        context = fact['Context']
        element_name = fact['Element Name']
        value = fact['Fact Value']

        # Check for specific, non-numeric patterns first
        if 'Employees_TableA' in context:
            # This is a pivot table, so we need to capture the dimensions
            gender = 'Total'  # Default
            if 'Male' in context:
                gender = 'Male'
            elif 'Female' in context:
                gender = 'Female'
            elif 'OtherGender' in context:
                gender = 'Other'

            emp_type = 'Overall'  # Default
            if 'PermanentEmployees' in context:
                emp_type = 'Permanent'
            elif 'OtherThanPermanentEmployees' in context:
                emp_type = 'Other Than Permanent'

            employee_details[emp_type][gender][element_name] = value
            continue  # Move to the next fact once categorized

        # Now, check for general info
        if 'Main' in context or 'Principle' in context:
            general_info[f"{element_name}_{context}"] = value
            continue  # Move to the next fact once categorized

        # Finally, check for patterns with numeric IDs
        match = id_pattern.search(context)
        if match:
            item_id = int(match.group(1))
            if 'HoldingSubsidiaryAssociateCompanies' in context:
                holdings_subsidiaries[item_id][element_name] = value
            elif 'CSRProjectsAxis' in context:
                csr_projects[item_id][element_name] = value
            # Add other numeric-ID based tables here if needed

    # --- Step 4: Create DataFrames and write to Excel sheets ---
    with pd.ExcelWriter(output_excel, engine='openpyxl') as writer:
        print("Writing 'General_Info' sheet...")
        if general_info:
            pd.Series(general_info).reset_index().rename(
                columns={'index': 'Field', 0: 'Value'}
            ).to_excel(writer, sheet_name='General_Info', index=False)

        print("Writing 'Holdings_Subsidiaries' sheet...")
        if holdings_subsidiaries:
            pd.DataFrame(holdings_subsidiaries.values()).to_excel(writer, sheet_name='Holdings_Subsidiaries',
                                                                  index=False)

        print("Writing 'CSR_Projects' sheet...")
        if csr_projects:
            pd.DataFrame(csr_projects.values()).to_excel(writer, sheet_name='CSR_Projects', index=False)

        print("Writing 'Employee_Details' sheet...")
        if employee_details:
            flat_employee_list = []
            for emp_type, genders in employee_details.items():
                for gender, details in genders.items():
                    details['EmployeeType'] = emp_type
                    details['Gender'] = gender
                    flat_employee_list.append(details)
            pd.DataFrame(flat_employee_list).to_excel(writer, sheet_name='Employee_Details', index=False)

    print(f"\nSuccessfully created Excel file at: {output_excel}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python brsr_xml_to_excel.py <input_xml_file> <output_excel_file>")
        sys.exit(1)

    input_xml = sys.argv[1]
    output_excel = sys.argv[2]

    parse_brsr_xml_to_excel(input_xml, output_excel)
