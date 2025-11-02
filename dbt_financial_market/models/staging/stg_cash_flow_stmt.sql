WITH extend_json AS(

    SELECT source,
           ingestion_ts,
           jsonb_array_elements(json_list::JSONB) AS elem
    FROM {{ source('raw','cash_flow_stmt') }}

),


renamed_and_typed AS (
    SELECT
       NULLIF( (elem->>'symbol'), '')::VARCHAR(50)                            AS symbol,
       NULLIF( (elem->>'date'), '')::date                                     AS date,
       TO_CHAR(NULLIF((elem->>'date'), '')::date, 'YYYYMMDD')::integer        AS date_key,
       NULLIF( (elem->>'reportedCurrency'), '')::VARCHAR(5)                   AS reported_currency,
       NULLIF( (elem->>'cik'), '')::VARCHAR(50)                               AS cik,
       NULLIF( (elem->>'filingDate'), '')::date                               AS filing_date,
       NULLIF( (elem->>'acceptedDate'), '')::timestamp::date                  AS accepted_date,
       NULLIF( (elem->>'fiscalYear'), '')::integer                            AS fiscal_year,
       NULLIF( (elem->>'period'), '')::VARCHAR(20)                            AS fiscal_period,
       CASE
            WHEN elem->>'period' = 'Q1' THEN 1
            WHEN elem->>'period' = 'Q2' THEN 2
            WHEN elem->>'period' = 'Q3' THEN 3
            WHEN elem->>'period' = 'Q4' THEN 4
            ELSE NULL
        END AS fiscal_quarter,

       {{ bigint_safe_cast_dict
          ({
                "elem->>'netIncome'"                                : "net_income",
                "elem->>'depreciationAndAmortization'"              : "depreciation_and_amortization",
                "elem->>'deferredIncomeTax'"                        : "deferred_income_tax",
                "elem->>'stockBasedCompensation'"                   : "stock_based_compensation",
                "elem->>'changeInWorkingCapital'"                   : "working_capital_changes",
                "elem->>'accountsReceivables'"                      : "accounts_receivables",
                "elem->>'inventory'"                                : "inventory",
                "elem->>'accountsPayables'"                         : "accounts_payables",
                "elem->>'otherWorkingCapital'"                      : "other_working_capital",
                "elem->>'otherNonCashItems'"                        : "other_non_cash_items",
                "elem->>'netCashProvidedByOperatingActivities'"     : "net_cash_from_operating_activities",
                "elem->>'investmentsInPropertyPlantAndEquipment'"   : "investments_in_property_plant_equipment",
                "elem->>'acquisitionsNet'"                          : "net_acquisitions",
                "elem->>'purchasesOfInvestments'"                   : "purchases_of_investments",
                "elem->>'salesMaturitiesOfInvestments'"             : "sales_maturities_of_investments",
                "elem->>'otherInvestingActivities'"                 : "other_investing_activities",
                "elem->>'netCashProvidedByInvestingActivities'"     : "net_cash_from_investing_activities",
                "elem->>'netDebtIssuance'"                          : "net_debt_issuance",
                "elem->>'longTermNetDebtIssuance'"                  : "long_term_net_debt_issuance",
                "elem->>'shortTermNetDebtIssuance'"                 : "short_term_net_debt_issuance",
                "elem->>'netStockIssuance'"                         : "net_stock_issuance",
                "elem->>'netCommonStockIssuance'"                   : "net_common_stock_issuance",
                "elem->>'commonStockIssuance'"                      : "common_stock_issuance",
                "elem->>'commonStockRepurchased'"                   : "common_stock_repurchased",
                "elem->>'netPreferredStockIssuance'"                : "net_preferred_stock_issuance",
                "elem->>'netDividendsPaid'"                         : "net_dividends_paid",
                "elem->>'commonDividendsPaid'"                      : "common_dividends_paid",
                "elem->>'preferredDividendsPaid'"                   : "preferred_dividends_paid",
                "elem->>'otherFinancingActivities'"                 : "other_financing_activities",
                "elem->>'netCashProvidedByFinancingActivities'"     : "net_cash_from_financing_activities",
                "elem->>'effectOfForexChangesOnCash'"               : "effect_of_forex_changes_on_cash",
                "elem->>'netChangeInCash'"                          : "net_change_in_cash",
                "elem->>'cashAtEndOfPeriod'"                        : "cash_at_end_of_period",
                "elem->>'cashAtBeginningOfPeriod'"                  : "cash_at_beginning_of_period",
                "elem->>'operatingCashFlow'"                        : "operating_cash_flow",
                "elem->>'capitalExpenditure'"                       : "capital_expenditure",
                "elem->>'freeCashFlow'"                             : "free_cash_flow",
                "elem->>'incomeTaxesPaid'"                          : "income_taxes_paid",
                "elem->>'interestPaid'"                             : "interest_paid"
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