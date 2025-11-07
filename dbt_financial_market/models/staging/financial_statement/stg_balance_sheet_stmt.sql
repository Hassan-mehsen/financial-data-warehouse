
{{  config( 

        contract={"enforced": True}
    ) 
}}

WITH extend_json AS(

    SELECT source,
           ingestion_ts,
           jsonb_array_elements(json_list::JSONB) AS elem
    FROM {{ source('raw','balance_sheet_stmt') }}

),

renamed_and_typed AS (
    SELECT

       NULLIF( (elem->>'symbol'), '')::VARCHAR(50)                                             AS symbol,
       NULLIF( (elem->>'date'), '')::date                                                      AS date,
       TO_CHAR(NULLIF((elem->>'date'), '')::date, 'YYYYMMDD')::integer                         AS date_key,
       NULLIF( (elem->>'reportedCurrency'), '')::VARCHAR(5)                                    AS reported_currency,
       NULLIF( (elem->>'cik'), '')::VARCHAR(50)                                                AS cik,
       NULLIF( (elem->>'filingDate'), '')::date                                                AS filing_date,
       NULLIF( (elem->>'acceptedDate'), '')::timestamp::date                                   AS accepted_date,
       NULLIF( (elem->>'fiscalYear'), '')::integer                                             AS fiscal_year,
       NULLIF( (elem->>'period'), '')::VARCHAR(20)                                             AS fiscal_period,
       CASE
            WHEN elem->>'period' = 'Q1' THEN 1
            WHEN elem->>'period' = 'Q2' THEN 2
            WHEN elem->>'period' = 'Q3' THEN 3
            WHEN elem->>'period' = 'Q4' THEN 4
            ELSE NULL
        END AS fiscal_quarter,

       {{ bigint_safe_cast_dict
          ({
                "elem->>'cashAndCashEquivalents'"          :   "cash_and_cash_equivalents",
                "elem->>'shortTermInvestments'"            :   "short_term_investments",
                "elem->>'cashAndShortTermInvestments'"     :   "cash_and_short_term_investments",
                "elem->>'netReceivables'"                  :   "net_receivables",
                "elem->>'accountsReceivables'"             :   "accounts_receivables",
                "elem->>'otherReceivables'"                :   "other_receivables",
                "elem->>'inventory'"                       :   "inventory",
                "elem->>'otherCurrentAssets'"              :   "other_current_assets",
                "elem->>'totalCurrentAssets'"              :   "total_current_assets",
                "elem->>'propertyPlantEquipmentNet'"       :   "property_plant_equipment_net",
                "elem->>'goodwill'"                        :   "goodwill",
                "elem->>'intangibleAssets'"                :   "intangible_assets",
                "elem->>'goodwillAndIntangibleAssets'"     :   "goodwill_and_intangible_assets",
                "elem->>'longTermInvestments'"             :   "long_term_investments",
                "elem->>'taxAssets'"                       :   "tax_assets",
                "elem->>'otherNonCurrentAssets'"           :   "other_non_current_assets",
                "elem->>'totalNonCurrentAssets'"           :   "total_non_current_assets",
                "elem->>'otherAssets'"                     :   "other_assets",
                "elem->>'totalAssets'"                     :   "total_assets",
                "elem->>'totalPayables'"                   :   "total_payables",
                "elem->>'accountPayables'"                 :   "account_payables",
                "elem->>'otherPayables'"                   :   "other_payables",
                "elem->>'accruedExpenses'"                 :   "accrued_expenses",
                "elem->>'shortTermDebt'"                   :   "short_term_debt",
                "elem->>'capitalLeaseObligationsCurrent'"  : "capital_lease_obligations_current",
                "elem->>'taxPayables'"                     :   "tax_payables",
                "elem->>'deferredRevenue'"                 :   "deferred_revenue",
                "elem->>'otherCurrentLiabilities'"         : "other_current_liabilities",
                "elem->>'totalCurrentLiabilities'"         : "total_current_liabilities",
                "elem->>'longTermDebt'"                    : "long_term_debt",
                "elem->>'deferredRevenueNonCurrent'"       : "deferred_revenue_non_current",
                "elem->>'deferredTaxLiabilitiesNonCurrent'": "deferred_tax_liabilities_non_current",
                "elem->>'otherNonCurrentLiabilities'"      : "other_non_current_liabilities",
                "elem->>'totalNonCurrentLiabilities'"      : "total_non_current_liabilities",
                "elem->>'otherLiabilities'"                : "other_liabilities",
                "elem->>'capitalLeaseObligations'"         : "capital_lease_obligations",
                "elem->>'totalLiabilities'"                : "total_liabilities",
                "elem->>'treasuryStock'"                   : "treasury_stock",
                "elem->>'preferredStock'"                  : "preferred_stock",
                "elem->>'commonStock'"                     : "common_stock",
                "elem->>'retainedEarnings'"                : "retained_earnings",
                "elem->>'additionalPaidInCapital'"         : "additional_paid_in_capital",
                "elem->>'accumulatedOtherComprehensiveIncomeLoss'": "accumulated_other_comprehensive_income_loss",
                "elem->>'otherTotalStockholdersEquity'"    : "other_total_stockholders_equity",
                "elem->>'totalStockholdersEquity'"         : "total_stockholders_equity",
                "elem->>'totalEquity'"                     : "total_equity",
                "elem->>'totalLiabilitiesAndTotalEquity'"  : "total_liabilities_and_equity",
                "elem->>'minorityInterest'"                : "minority_interest",
                "elem->>'totalInvestments'"                : "total_investments",
                "elem->>'totalDebt'"                       : "total_debt",
                "elem->>'netDebt'"                         : "net_debt",
          }) 
       }},

       source,
       ingestion_ts

    FROM extend_json
)

SELECT *
FROM renamed_and_typed
WHERE date_key IS NOT NULL 
 OR fiscal_quarter IS NOT NULL
 AND symbol IS NOT NULL
