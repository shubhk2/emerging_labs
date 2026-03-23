import logging
from typing import Dict, Any, List
from psycopg2.extras import RealDictCursor
from backend.db_setup import connect_to_db

logger = logging.getLogger(__name__)


def get_rpt_data(company_number: int) -> Dict[str, Any]:
    """
    Returns Related Party Transactions (RPT) details for a given company_number
    excluding 'id' and 'company_no'.
    """
    conn = connect_to_db()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        # Fetch company details
        cursor.execute(
            """
            SELECT full_name, ticker
            FROM public.company_detail
            WHERE id = %s
            ORDER BY id
            LIMIT 1
            """,
            (company_number,),
        )
        company_details = cursor.fetchone()
        if not company_details:
            logger.warning(f"No company found for company_number={company_number}")
            return {"error": f"No company found for {company_number}"}

        # Fetch RPT data excluding id and company_no
        cursor.execute(
            """
            SELECT 
                "TransactionID",
                "NameOfCounterParty",
                "RelationshipOfTheCounterpartyWithTheListedEntityOrItsSubsidiary",
                "TypeOfRelatedPartyTransaction",
                "AmountOfRelatedPartyTransactionDuringTheReportingPeriod",
                "AmountOfRelatedPartyTransaction_Outstanding",
                "AmountOfRelatedPartyTransaction_PreviousYear",
                "ValueOfTheRelatedPartyTransactionAsApprovedByTheAuditCommittee",
                "DetailsOfOtherRelatedPartyTransaction",
                "RemarksOnApprovalByAuditCommittee",
                "NameOfListedEntityOrSubsidiaryEnteringIntoTheTransaction",
                "CompanyName",
                "ScripCode",
                "RelatedPartyTransactionExplanatory",
                "NatureOfTheLoansOrInterCorporateDepositsOrAdvancesOrInvestments",
                "IROfLoansOrInterCorporateDepositsOrAdvancesOrInvestments",
                "TenureOfLoansOrInterCorporateDepositsOrAdvancesOrInvestments",
                "TypeOfOfLoansOrICDOrAdvancesOrInvestmentsSecuredOrUnsecured",
                "PurposeOfUtilisationOfTheUltimateRecipientOfFundsForEndusage",
                "NatureOfFinancialIndebtedness",
                "CostOfFinancialIndebtedness",
                "TenureOfFinancialIndebtedness",
                "PANOfListedEntityOrSubsidiaryEnteringIntoTheTransaction",
                "PANOfCounterParty"
            FROM public.rpt
            WHERE company_no = %s
            ORDER BY id
            """,
            (company_number,),
        )
        rows = cursor.fetchall()

        headers: List[str] = [
            "TransactionID",
            "NameOfCounterParty",
            "RelationshipOfTheCounterpartyWithTheListedEntityOrItsSubsidiary",
            "TypeOfRelatedPartyTransaction",
            "AmountOfRelatedPartyTransactionDuringTheReportingPeriod",
            "AmountOfRelatedPartyTransaction_Outstanding",
            "AmountOfRelatedPartyTransaction_PreviousYear",
            "ValueOfTheRelatedPartyTransactionAsApprovedByTheAuditCommittee",
            "DetailsOfOtherRelatedPartyTransaction",
            "RemarksOnApprovalByAuditCommittee",
            "NameOfListedEntityOrSubsidiaryEnteringIntoTheTransaction",
            "CompanyName",
            "ScripCode",
            "RelatedPartyTransactionExplanatory",
            "NatureOfTheLoansOrInterCorporateDepositsOrAdvancesOrInvestments",
            "IROfLoansOrInterCorporateDepositsOrAdvancesOrInvestments",
            "TenureOfLoansOrInterCorporateDepositsOrAdvancesOrInvestments",
            "TypeOfOfLoansOrICDOrAdvancesOrInvestmentsSecuredOrUnsecured",
            "PurposeOfUtilisationOfTheUltimateRecipientOfFundsForEndusage",
            "NatureOfFinancialIndebtedness",
            "CostOfFinancialIndebtedness",
            "TenureOfFinancialIndebtedness",
            "PANOfListedEntityOrSubsidiaryEnteringIntoTheTransaction",
            "PANOfCounterParty",
        ]

        formatted_data: List[Dict[str, Any]] = []
        for row in rows:
            formatted_data.append({key: row.get(key) for key in headers})

        return {
            "company_name": company_details["full_name"],
            "ticker": company_details["ticker"],
            "headers": headers,
            "data": formatted_data,
        }
    except Exception as e:
        logger.error(f"Error in get_rpt_data: {e}", exc_info=True)
        return {"error": str(e)}
    finally:
        cursor.close()
        conn.close()

