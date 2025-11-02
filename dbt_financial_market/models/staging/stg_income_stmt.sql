WITH extend_json AS(

    SELECT source,
           ingestion_ts,
           jsonb_array_elements(json_list::JSONB) AS elem
    FROM {{ source('raw','income_stmt') }}

),

renamed_and_typed AS (
    SELECT
       NULLIF( (elem->>'symbol'), '')::VARCHAR(50)                                         AS symbol,
       NULLIF( (elem->>'date'), '')::date                                                  AS date,
       TO_CHAR(NULLIF((elem->>'date'), '')::date, 'YYYYMMDD')::integer                     AS date_key,
       NULLIF( (elem->>'reportedCurrency'), '')::VARCHAR(5)                                AS reported_currency,
       NULLIF( (elem->>'cik'), '')::VARCHAR(50)                                            AS cik,
       NULLIF( (elem->>'filingDate'), '')::date                                            AS filing_date,
       NULLIF( (elem->>'acceptedDate'), '')::timestamp::date                               AS accepted_date,
       NULLIF( (elem->>'fiscalYear'), '')::integer                                         AS fiscal_year,
       NULLIF( (elem->>'period'), '')::VARCHAR(20)                                         AS fiscal_period,
       CASE
            WHEN elem->>'period' = 'Q1' THEN 1
            WHEN elem->>'period' = 'Q2' THEN 2
            WHEN elem->>'period' = 'Q3' THEN 3
            WHEN elem->>'period' = 'Q4' THEN 4
            ELSE NULL
        END AS fiscal_quarter,

       {{ bigint_safe_cast_dict 
          ({
                "elem->>'revenue'"                             : "revenue",
                "elem->>'costOfRevenue'"                       : "cost_of_revenue",
                "elem->>'grossProfit'"                         : "gross_profit",
                "elem->>'researchAndDevelopmentExpenses'"      : "rd_expenses",
                "elem->>'generalAndAdministrativeExpenses'"    : "general_admin_expenses",
                "elem->>'sellingAndMarketingExpenses'"         : "selling_marketing_expenses",
                "elem->>'sellingGeneralAndAdministrativeExpenses'" : "sga_expenses",
                "elem->>'otherExpenses'"                       : "other_expenses",
                "elem->>'operatingExpenses'"                   : "operating_expenses",
                "elem->>'costAndExpenses'"                     : "cost_and_expenses",
                "elem->>'netInterestIncome'"                   : "net_interest_income",
                "elem->>'interestIncome'"                      : "interest_income",
                "elem->>'interestExpense'"                     : "interest_expense",
                "elem->>'depreciationAndAmortization'"         : "depreciation_and_amortization",
                "elem->>'ebitda'"                              : "ebitda",
                "elem->>'ebit'"                                : "ebit",
                "elem->>'nonOperatingIncomeExcludingInterest'" : "non_operating_income_without_interest",
                "elem->>'totalOtherIncomeExpensesNet'"         : "total_other_income_expenses_net",
                "elem->>'incomeBeforeTax'"                     : "income_before_tax",
                "elem->>'netIncomeFromContinuingOperations'"   : "net_income_continuing_ops",
                "elem->>'netIncomeFromDiscontinuedOperations'" : "net_income_discontinued_ops",
                "elem->>'otherAdjustmentsToNetIncome'"         : "other_adjustments_net_income",
                "elem->>'netIncome'"                           : "net_income",
                "elem->>'netIncomeDeductions'"                 : "net_income_deductions",
                "elem->>'weightedAverageShsOut'"               : "weighted_avg_shares_out",
                "elem->>'weightedAverageShsOutDil'"            : "weighted_avg_shares_out_diluted",
            })
        }},

       NULLIF( (elem->>'eps'), '')::numeric(12,6)                                          AS eps,
       NULLIF( (elem->>'epsDiluted'), '')::numeric(12,6)                                   AS eps_diluted,
       source,
       ingestion_ts
       
    FROM extend_json
)

SELECT *
FROM renamed_and_typed
WHERE date_key IS NOT NULL 
 OR fiscal_quarter IS NOT NULL
 AND symbol IS NOT NULL