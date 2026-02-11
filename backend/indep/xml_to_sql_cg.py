import os
import sys
import xml.etree.ElementTree as ET
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import re
from collections import defaultdict


def get_company_no(ticker, engine):
    """Fetches the company_no from the company_detail table for a given ticker."""
    try:
        with engine.connect() as connection:
            query = text("SELECT id FROM public.company_detail WHERE ticker = :ticker")
            result = connection.execute(query, {'ticker': ticker}).fetchone()
            if result:
                return result[0]
            else:
                print(f"  WARNING: Ticker '{ticker}' not found in company_detail table.")
                return None
    except Exception as e:
        print(f"  ERROR: Database query failed for ticker '{ticker}'. Reason: {e}")
        return None


def process_cg_files():
    """Parses all CG XML files and loads the data into the database."""
    load_dotenv()
    db_url = os.getenv("POSTGRES_URL")
    if not db_url:
        print("ERROR: POSTGRES_URL not found in .env file.")
        sys.exit(1)

    try:
        engine = create_engine(db_url)
    except Exception as e:
        print(f"ERROR: Failed to create database engine. Reason: {e}")
        sys.exit(1)

    script_dir = os.path.dirname(os.path.abspath(__file__))
    cg_folder = os.path.abspath(os.path.join(script_dir, '..', 'cg'))

    if not os.path.isdir(cg_folder):
        print(f"ERROR: Directory not found: {cg_folder}")
        sys.exit(1)

    for filename in os.listdir(cg_folder):
        if not filename.endswith(".xml") or ':Zone.Identifier' in filename:
            continue

        file_path = os.path.join(cg_folder, filename)
        print(f"\nProcessing file: {filename}...")

        try:
            ticker = os.path.splitext(filename)[0]
            company_no = get_company_no(ticker, engine)
            if company_no is None:
                continue

            tree = ET.parse(file_path)
            root = tree.getroot()

            facts = []
            for elem in root.iter():
                if elem.text and elem.text.strip():
                    element_name = elem.tag.split('}')[-1]
                    context_ref = elem.get('contextRef', '')
                    fact_value = elem.text.strip()
                    if context_ref:
                        facts.append({'Element Name': element_name, 'Context': context_ref, 'Fact Value': fact_value})

            # --- Process Board Composition ---
            board_composition = defaultdict(dict)
            for fact in facts:
                if 'CompBOD' in fact['Context']:
                    match = re.search(r'(\d+)$', fact['Context'])
                    if match:
                        board_composition[int(match.group(1))][fact['Element Name']] = fact['Fact Value']

            if board_composition:
                df = pd.DataFrame(board_composition.values())
                df['company_no'] = company_no
                df.rename(columns={
                    'NameOftheDirector': 'director_name', 'DirectorIdentificationNumberOfDirector': 'din',
                    'PermanentAccountNumberOfDirector': 'pan', 'PositionOfDirectorInBoardOne': 'category',
                    'PositionOfDirectorInBoardTwo': 'designation', 'DateOfAppointmentOfDirector': 'appointment_date',
                    'DateOfReappointmentOfDirector': 'reappointment_date',
                    'DateOfCessationOfDirector': 'cessation_date',
                    'TenureOfDirector': 'tenure', 'DateOfBirth': 'date_of_birth',
                    'NumberOfDirectorshipInListedEntitiesIncludingThisListedEntity': 'directorships_in_listed_entities',
                    'NumberOfMembershipsInAuditOrStakeholderCommitteesIncludingThisListedEntity': 'memberships_in_committees',
                    'NumberOfPostOfChairpersonInAuditOrStakeholderCommitteeHeldInListedEntitiesIncludingThisListedEntity': 'chairmanships_in_committees',
                    'ReasonForCessation': 'reason_for_cessation'
                }, inplace=True)

                db_cols = ['company_no', 'director_name', 'din', 'pan', 'category', 'designation', 'appointment_date',
                           'reappointment_date', 'cessation_date', 'tenure', 'date_of_birth',
                           'directorships_in_listed_entities', 'memberships_in_committees',
                           'chairmanships_in_committees', 'reason_for_cessation']
                for col in db_cols:
                    if col not in df.columns: df[col] = None  # Add missing columns
                df = df[db_cols]  # Ensure correct order
                df.to_sql('cg_board_composition', engine, schema='public', if_exists='append', index=False)
                print(f"  SUCCESS: Inserted {len(df)} records into cg_board_composition.")

            # --- Process Committee Composition ---
            committee_composition = defaultdict(dict)
            for fact in facts:
                if 'CompComit' in fact['Context']:
                    match = re.search(r'(\d+)$', fact['Context'])
                    if match:
                        committee_composition[int(match.group(1))][fact['Element Name']] = fact['Fact Value']

            if committee_composition:
                df = pd.DataFrame(committee_composition.values())
                df['company_no'] = company_no
                df.rename(columns={
                    'NameOfCommittee': 'committee_name', 'NameOfCommitteeMembers': 'director_name',
                    'DirectorIdentificationNumberOfDirector': 'din', 'PositionOfDirectorInCommitteeOne': 'category',
                    'PositionOfDirectorInCommitteeTwo': 'position_in_committee',
                    'DateOfAppointmentOfDirectorInCommittee': 'appointment_date',
                    'DateOfCessationOfDirectorInCommittee': 'cessation_date',
                    'DisclosureOfNotesOnCommitteeTextBlock': 'notes'
                }, inplace=True)
                db_cols = ['company_no', 'committee_name', 'director_name', 'din', 'category', 'position_in_committee',
                           'appointment_date', 'cessation_date', 'notes']
                for col in db_cols:
                    if col not in df.columns: df[col] = None
                df = df[db_cols]
                df.to_sql('cg_committee_composition', engine, schema='public', if_exists='append', index=False)
                print(f"  SUCCESS: Inserted {len(df)} records into cg_committee_composition.")

            # --- Process Board Meetings ---
            board_meetings = defaultdict(dict)
            for fact in facts:
                if 'MeetingBOD' in fact['Context']:
                    match = re.search(r'(\d+)$', fact['Context'])
                    if match:
                        board_meetings[int(match.group(1))][fact['Element Name']] = fact['Fact Value']

            if board_meetings:
                df = pd.DataFrame(board_meetings.values())
                df['company_no'] = company_no
                df['meeting_date'] = df['DatesOfMeetingInThePreviousQuarter'].fillna(
                    df['DatesOfMeetingIfAnyInTheRelevantQuarter'])
                df['meeting_type'] = df.apply(lambda row: 'Previous Quarter' if pd.notna(
                    row['DatesOfMeetingInThePreviousQuarter']) else 'Relevant Quarter', axis=1)
                df.rename(columns={
                    'WhetherRequirementOfQuorumMet': 'quorum_met',
                    'TotalNumberOfDirectorsAsOnDateOfTheMeeting': 'directors_on_meeting_date',
                    'NumberOfDirectorsPresentInMeetingOfBoardOfDirectors': 'directors_present',
                    'NumberOfIndependentDirectorsAttendingTheMeeting': 'independent_directors_present',
                    'MaximumGapBetweenAnyTwoConsecutiveMeetings': 'gap_between_meetings_days'
                }, inplace=True)
                db_cols = ['company_no', 'meeting_date', 'meeting_type', 'quorum_met', 'directors_on_meeting_date',
                           'directors_present', 'independent_directors_present', 'gap_between_meetings_days']
                for col in db_cols:
                    if col not in df.columns: df[col] = None
                df = df[db_cols]
                df.to_sql('cg_board_meetings', engine, schema='public', if_exists='append', index=False)
                print(f"  SUCCESS: Inserted {len(df)} records into cg_board_meetings.")

            # --- Process Committee Meetings ---
            committee_meetings = defaultdict(dict)
            for fact in facts:
                if 'MeetingComit' in fact['Context']:
                    match = re.search(r'(\d+)$', fact['Context'])
                    if match:
                        committee_meetings[int(match.group(1))][fact['Element Name']] = fact['Fact Value']

            if committee_meetings:
                df = pd.DataFrame(committee_meetings.values())
                df['company_no'] = company_no
                df['meeting_date'] = df['DatesOfMeetingOfTheCommitteeInThePreviousQuarter'].fillna(
                    df['DatesOfMeetingOfTheCommitteeInTheRelevantQuarter'])
                df['meeting_type'] = df.apply(lambda row: 'Previous Quarter' if pd.notna(
                    row['DatesOfMeetingOfTheCommitteeInThePreviousQuarter']) else 'Relevant Quarter', axis=1)
                df.rename(columns={
                    'NameOfCommittee': 'committee_name', 'WhetherRequirementOfQuorumMet': 'quorum_met',
                    'TotalNumberOfDirectorsAsOnDateOfTheMeeting': 'directors_on_meeting_date',
                    'NumberOfDirectorPresentInMeetingOfCommitteeAllDirectorsIncludingIndependentDirector': 'directors_present',
                    'NumberOfIndependentDirectorAttendingMeetingOfCommittee': 'independent_directors_present',
                    'MaximumGapBetweenAnyTwoConsecutiveMeetings': 'gap_between_meetings_days'
                }, inplace=True)
                db_cols = ['company_no', 'committee_name', 'meeting_date', 'meeting_type', 'quorum_met',
                           'directors_on_meeting_date', 'directors_present', 'independent_directors_present',
                           'gap_between_meetings_days']
                for col in db_cols:
                    if col not in df.columns: df[col] = None
                df = df[db_cols]
                df.to_sql('cg_committee_meetings', engine, schema='public', if_exists='append', index=False)
                print(f"  SUCCESS: Inserted {len(df)} records into cg_committee_meetings.")

        except Exception as e:
            print(f"  ERROR: Failed to process file {filename}. Reason: {e}")


if __name__ == "__main__":
    process_cg_files()
