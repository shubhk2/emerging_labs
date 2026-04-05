import { CREATE_CHAT_HISTORY, ERROR, LOADING, REMOVE_CHAT_HISTORY, SET_CHAT_HISTORY, SET_CURRENT_LLM_RESPONSE } from "./actionTypes";

const initState = {
    isLoading: false,
    isError: false,
    chatHistory: [12],
    chatHistoryMap: {
        12: {
            title: "Infosys Vs Mahindra",
            history: [
                {
                    query: "What is the difference between Infosys and Mahindra?",
                    response: {
                        "llm_response": "Okay, here is a financial comparison of TCS and JIOFIN based on the available data.\n\n**📊 Key Metrics Comparison**\n~COMPARISON_TABLE~\n\n**💡 Performance Analysis**\n\nBased on the provided data:\n\n*   **Revenue Growth:** JIOFIN shows significantly higher percentage revenue growth in FY24 (3460%) and FY25 (830%) compared to TCS (FY24: 680%, FY25: 600%). However, this is likely due to JIOFIN starting from a much smaller base and potentially reflecting structural changes or specific large transactions, as historical JIOFIN revenue figures are highly volatile and sometimes negative. TCS demonstrates more consistent, albeit moderate, revenue growth (FY25: 4.2% in constant currency mentioned in earnings call).\n*   **Profitability:** TCS exhibits stable and strong operating and net margins (FY25: 24.3% Operating Margin, 19% Net Margin). JIOFIN also shows very high gross, operating, and net margins in FY24 and FY25, but these are characteristic of a financial services business model which differs significantly from IT services. TCS's EBITDA margin is also consistently strong.\n*   **Efficiency:** TCS shows stable asset turnover and receivables turnover ratios. JIOFIN's efficiency ratios are highly variable historically, reflecting its different business nature and potentially recent operational changes.\n*   **Liquidity:** TCS maintains strong current and quick ratios, indicating good short-term liquidity. JIOFIN's liquidity ratios are extremely high in recent years, particularly the current ratio, which could indicate a large proportion of current assets (like cash or short-term investments) relative to current liabilities.\n*   **Leverage:** Both companies appear to have relatively low debt-to-asset ratios, suggesting conservative capital structures.\n*   **Cash Flow:** TCS consistently generates strong positive cash flow from operations and free cash flow. JIOFIN's cash flow from operations has been volatile historically, showing a large negative figure in FY24 before turning positive in the FY25 forecast. TCS's capex is a small, consistent portion of its operating cash flow.\n*   **Earnings Per Share (EPS):** TCS shows consistent growth in EPS over the years. JIOFIN's historical EPS is highly volatile (including negative values), but shows positive and stable (though low in absolute terms compared to TCS) EPS in FY24 and FY25.\n*   **Business Focus:** TCS is a global IT services and consulting company with a significant presence in the BFSI sector, highlighted by products like TCS BaNCS. JIOFIN is a financial services company. This fundamental difference in business models drives many of the observed differences in financial metrics and ratios.\n\n**🎯 Investment Implications**\n\n*   **Value Proposition:** TCS offers a value proposition based on stability, consistent performance, proven execution, and shareholder returns (dividends, buybacks). JIOFIN's value proposition is centered around potentially high growth within the financial services sector, albeit with a less established track record in its current form.\n*   **Risk Considerations:** TCS faces risks typical of a large IT services firm, including global economic slowdowns impacting IT spending, currency fluctuations, and intense competition. JIOFIN faces risks inherent to financial services, such as credit risk, interest rate risk, regulatory changes, and market volatility. The limited historical data for JIOFIN in its current structure also presents a higher uncertainty risk compared to the established track record of TCS.\n*   **Suitability for Different Investor Types:**\n    *   **TCS:** Likely more suitable for investors seeking stability, consistent earnings, reliable dividends, and exposure to the global technology services sector. It might appeal to value or dividend-focused investors.\n    *   **JIOFIN:** May be more suitable for growth-oriented investors willing to accept higher risk for the potential of significant capital appreciation in the financial services sector. It might appeal to investors looking for exposure to newer or rapidly evolving segments of the market.\n\nIn summary, TCS represents a mature, stable, and highly profitable IT services giant with a consistent financial track record. JIOFIN, while showing high percentage growth and strong margins in recent/forecasted data, appears to be a newer or restructured entity in the financial services sector with a more volatile historical performance, offering higher potential growth but also higher risk due to its different business model and limited consistent history. The choice between them depends heavily on an investor's risk tolerance and investment objectives (stability vs. high growth potential).",
                        "enhanced_context_data": {
                            "classification": {
                                "chart_endpoint_type": "ratios",
                                "chart_parameters": [
                                    "net_profit_margin",
                                    "gross_profit_margin",
                                    "return_on_equity",
                                    "return_on_assets",
                                    "current_ratio",
                                    "debt_to_equity",
                                    "asset_turnover",
                                    "ebitda_margin"
                                ],
                                "confidence": 0.95,
                                "data_source_type": "hybrid",
                                "display_components": {
                                    "chart": true,
                                    "company_overview": false,
                                    "llm_response": true,
                                    "table": true
                                },
                                "endpoint_mode": "base",
                                "endpoint_type": "ratios",
                                "financial_metrics": [],
                                "identified_parameters": {
                                    "balance_sheet": [
                                        "total assets",
                                        "total equity",
                                        "cash and equivalents",
                                        "total liabilities"
                                    ],
                                    "cashflow": [
                                        "cash from operations",
                                        "free cash flow",
                                        "capital expenditure"
                                    ],
                                    "financial_ratios": [
                                        "net_profit_margin",
                                        "gross_profit_margin",
                                        "return_on_equity",
                                        "return_on_assets",
                                        "current_ratio",
                                        "debt_to_equity",
                                        "asset_turnover",
                                        "ebitda_margin"
                                    ],
                                    "profit_and_loss": [
                                        "net income",
                                        "total revenues",
                                        "operating income",
                                        "gross profit",
                                        "ebitda"
                                    ],
                                    "shareholder": []
                                },
                                "parameter_filters": {
                                    "balance_sheet": [
                                        "total assets",
                                        "total equity",
                                        "cash and equivalents",
                                        "total liabilities"
                                    ],
                                    "balance_sheet_related": [
                                        "short term investments",
                                        "total current assets",
                                        "preferred stock redeemable"
                                    ],
                                    "cashflow": [
                                        "cash from operations",
                                        "free cash flow",
                                        "capital expenditure"
                                    ],
                                    "cashflow_related": [
                                        "sale of property, plant, and equipment",
                                        "net income",
                                        "(gain) loss on sale of asset"
                                    ],
                                    "financial_ratios": [
                                        "net_profit_margin",
                                        "gross_profit_margin",
                                        "return_on_equity",
                                        "return_on_assets",
                                        "current_ratio",
                                        "debt_to_equity",
                                        "asset_turnover",
                                        "ebitda_margin"
                                    ],
                                    "financial_ratios_related": [
                                        "operating_profit_margin",
                                        "gross_profit_margin",
                                        "equity_ratio"
                                    ],
                                    "profit_and_loss": [
                                        "net income",
                                        "total revenues",
                                        "operating income",
                                        "gross profit",
                                        "ebitda"
                                    ],
                                    "profit_and_loss_related": [
                                        "operating income",
                                        "gross profit",
                                        "net income"
                                    ]
                                },
                                "query_type": "ratio_analysis",
                                "reasoning": "Identified SQL tables: balance_sheet, profit_and_loss, cashflow, financial_ratios; Identified 20 specific parameters; Vector stores needed: earnings",
                                "required_sql_tables": [
                                    "balance_sheet",
                                    "profit_and_loss",
                                    "cashflow",
                                    "financial_ratios"
                                ],
                                "vector_stores_needed": [
                                    "earnings"
                                ]
                            },
                            "combined_context": "[Earnings Call] --- Page 4 ---\nTata Consultancy Services Earnings Conference Call\nJuly 11, 2024, 19:00 hrs IST\nI'm now going to talk about our industry leading portfolio, our production\nplatform which saw good traction during the quarter.\n• \nignio™, our cognitive automation software suite, saw 24 new deal wins\nand 10 go-lives.\no \nOur R&I efforts have led in winning 60 patents so far in this\nproduct.\n• \nTCS BaNCS™, our flagship product for financial services, had 5 wins\nand 12 go-lives during the quarter.\no \nCore change drivers for our product continue to be business agility,\ncloud- readiness, enablement of easy integration to ecosystem and\npartner solutions, and enhanced client experience.\no \nSpecific to the Securities industry, ‘T+1’ and shortening of settlement\ncycles in general is a big growth driver for us. Custodians are also\nlooking to see how to improve accuracy in the capture of corporate\nannouncements and bring overall efficiency in the value chain.\no\n\n[Earnings Call] --- Page 5 ---\nTata Consultancy Services Earnings Conference Call\nJanuary 09, 2025, 19:00 hrs IST (08:30 hrs US ET)\n• \nTCS BaNCS™, our flagship product for financial services, continued its\nleadership with 4 wins and 7 go-lives during the quarter.\no \nThis quarter, we successfully completed Zions Bancorporations’ multiyear\nFutureCore project with TCS BaNCS. This large-scale core banking\nmodernization positions Zions, a leading financial services company in US at\nthe forefront of the industry. It enables real-time transaction processing and\ndelivers an exceptional customer experience. The project underscores the\npower of collaboration, positioning Zions as an innovator allowing for quick\nresponses to market demand and customer needs and driving increased\nrevenue.\n• \nTCS BFSI Products and Platforms saw 3 wins and 2 go-lives during the\nquarter.\no \nWe signed a 15-year contract with Ireland's Department of Social Pension to\n\n[Earnings Call] leadership team has been part of TCS' extraordinary journey, growing 14 times\nin the last two decades. We extend our heartfelt gratitude to all our current and\nformer employees, partners, customers and other stakeholders who have\nsupported us over the past 57 years.\nOur financial year 2025 revenue grew by 4.2% in constant currency. Our\noperating margin for the year came in at 24.3%. Our net margin was at 19%.\nComing to our more recent performance, our Q4 revenues grew 2.5% on a\nconstant currency basis. All major markets grew sequentially. Our record Q4\nTCV of $12.2 billion demonstrates our ability to gain market share. North\nAmerica TCV reached an all-time high of $6.8 billion. BFSI TCV was $4\nbillion, and Consumer Business Group contributed $1.7 billion. This\nimpressive performance stands out in the absence of mega deals.\nI'll now invite Samir and Milind to go over different aspects of our performance\nduring the quarter. I'll step in later to provide more color on the demand trends\n\n[Earnings Call] --- Page 25 ---\nTata Consultancy Services Earnings Conference Call\nApril 10, 2025, 19:00hrs IST (08:30hrs US ET)\nK Krithivasan: \nIt has been steady and slowly increasing.\nKumar Rakesh: \nPerfect. Thanks a lot.\nModerator: \nThank you. Ladies and gentlemen, that was the last question for today. I now\nhand the conference over to management for closing comments. Over to you.\nK Krithivasan: \nThank you, moderator.\nIn FY '25, our revenue grew by 4.2% year-on-year in constant currency, with\nan operating margin of 24.3% and a net margin of 19%.\nWe are pleased with the strong TCV of 12.2 billion in Q4, with sharp uptick\nseen across markets and industries.\nWe are monitoring the global situation very closely. We continue to stay close\nto our clients and strive to be the partner of relevance at all times. I would like\nto thank all TCSers for their effort and unwavering dedication to realizing their\nown and company's potential.\n\n[Earnings Call] --- Page 13 ---\nTata Consultancy Services Earnings Conference Call\nOctober 10, 2024, 19:00 hrs IST (09:30 hrs US ET)\nacross applications, infrastructure, engineering and business operations \nfor our customers. \n• Our client relationships are more about building resilience together. As\nwe face unprecedented global challenges, the strategic approach to \ncollaboration will prove crucial for long-term success and improving \nmarket share.  \n• TCV in Q2 was at $8.6 billion. The BFSI TCV was at $2.9 billion, while\nTCV for our consumer business group was at $1.2 billion. The TCV of \ndeals signed in North America stood at $4.4 billion.\nThe gradual easing of inflation in the major markets, improving \nmacroeconomic trends and expectations of a good holiday season \nspend among consumers give us hope and optimism around the \nprospect of improved discretionary spend and capital investments by \nour customers, which should bode well for our business.\n\n=== QUANTITATIVE DATA ===\n\n--- Profit And Loss Data ---\n\n--- Data for TCS ---\nOther Unusual Items for year 2016 was N/A and for year 2017 was N/A and for year 2018 was N/A and for year 2019 was N/A and for year 2020 was 13.0 cr and for year 2021 was 100.0 cr and for year 2022 was 7.0 cr and for year 2023 was -2.0 cr and for year 2024 was 7.0 cr and for year 2025 was 15.0 cr for company=TCS\n\nTotal Revenues % Chg. for year 2016 was 1480.0% and for year 2017 was 860.0% and for year 2018 was 440.00000000000006% and for year 2019 was 1900.0% and for year 2020 was 720.0% and for year 2021 was 459.99999999999994% and for year 2022 was 1680.0% and for year 2023 was 1760.0000000000002% and for year 2024 was 680.0% and for year 2025 was 600.0% for company=TCS\n\nGain (Loss) On Sale Of Assets for year 2016 was 5.0 cr and for year 2017 was 3.0 cr and for year 2018 was 25.0 cr and for year 2019 was 84.0 cr and for year 2020 was 46.0 cr and for year 2021 was 13.0 cr and for year 2022 was 23.0 cr and for year 2023 was 26.0 cr and for year 2024 was 7.0 cr and for year 2025 was 20.0 cr for company=TCS\n\nLegal Settlements for year 2016 was N/A and for year 2017 was N/A and for year 2018 was N/A and for year 2019 was N/A and for year 2020 was N/A and for year 2021 was -1218.0 cr and for year 2022 was N/A and for year 2023 was N/A and for year 2024 was -958.0 cr and for year 2025 was N/A for company=TCS\n\nEPS Diluted for year 2016 was 6.16 cr and for year 2017 was 6.67 cr and for year 2018 was 6.709999999999999 cr and for year 2019 was 8.3 cr and for year 2020 was 8.620000000000001 cr and for year 2021 was 8.67 cr and for year 2022 was 10.36 cr and for year 2023 was 11.52 cr and for year 2024 was 12.59 cr and for year 2025 was 13.419999999999998 cr for company=TCS\n\nOperating Revenues for year 2016 was 108646.0 cr and for year 2017 was 117966.0 cr and for year 2018 was 123104.0 cr and for year 2019 was 146463.0 cr and for year 2020 was 156949.0 cr and for year 2021 was 164177.0 cr and for year 2022 was 191754.0 cr and for year 2023 was 225458.0 cr and for year 2024 was 240893.0 cr and for year 2025 was 255324.0 cr for company=TCS\n\nTotal Revenues for year 2016 was 108646.0 cr and for year 2017 was 117966.0 cr and for year 2018 was 123104.0 cr and for year 2019 was 146463.0 cr and for year 2020 was 156949.0 cr and for year 2021 was 164177.0 cr and for year 2022 was 191754.0 cr and for year 2023 was 225458.0 cr and for year 2024 was 240893.0 cr and for year 2025 was 255324.0 cr for company=TCS\n\nCost of Goods Sold, Total for year 2016 was 55348.0 cr and for year 2017 was 64429.0 cr and for year 2018 was 69096.0 cr and for year 2019 was 80516.0 cr and for year 2020 was 87857.0 cr and for year 2021 was 93276.0 cr and for year 2022 was 108717.0 cr and for year 2023 was 129403.0 cr and for year 2024 was 143833.0 cr and for year 2025 was 157436.0 cr for company=TCS\n\nGross Profit for year 2016 was 53298.0 cr and for year 2017 was 53537.0 cr and for year 2018 was 54008.0 cr and for year 2019 was 65947.0 cr and for year 2020 was 69092.0 cr and for year 2021 was 70901.0 cr and for year 2022 was 83037.0 cr and for year 2023 was 96055.0 cr and for year 2024 was 97060.0 cr and for year 2025 was 97888.0 cr for company=TCS\n\nGross Profit Margin for year 2016 was 4.91 cr and for year 2017 was 4.54 cr and for year 2018 was 4.39 cr and for year 2019 was 4.5 cr and for year 2020 was 4.4 cr and for year 2021 was 4.32 cr and for year 2022 was 4.33 cr and for year 2023 was 4.26 cr and for year 2024 was 4.029999999999999 cr and for year 2025 was 3.8299999999999996 cr for company=TCS\n\nSelling General & Admin Expenses, Total for year 2016 was N/A and for year 2017 was 3685.0 cr and for year 2018 was 3938.0 cr and for year 2019 was 4262.0 cr and for year 2020 was 2702.0 cr and for year 2021 was 2131.0 cr and for year 2022 was 2139.0 cr and for year 2023 was 2655.0 cr and for year 2024 was 3100.0 cr and for year 2025 was 3456.0 cr for company=TCS\n\nDepreciation & Amortization for year 2016 was 1888.0 cr and for year 2017 was 1987.0 cr and for year 2018 was 2014.0 cr and for year 2019 was 2056.0 cr and for year 2020 was 3529.0 cr and for year 2021 was 4065.0 cr and for year 2022 was 4604.0 cr and for year 2023 was 5022.0 cr and for year 2024 was 4985.0 cr and for year 2025 was 5242.0 cr for company=TCS\n\nOther Operating Expenses for year 2016 was 22596.0 cr and for year 2017 was 17524.0 cr and for year 2018 was 17538.0 cr and for year 2019 was 22173.0 cr and for year 2020 was 24280.0 cr and for year 2021 was 22223.0 cr and for year 2022 was 27850.0 cr and for year 2023 was 34141.0 cr and for year 2024 was 29664.0 cr and for year 2025 was 27022.0 cr for company=TCS\n\nOther Operating Expenses, Total for year 2016 was 24484.0 cr and for year 2017 was 23196.0 cr and for year 2018 was 23490.0 cr and for year 2019 was 28491.0 cr and for year 2020 was 30511.0 cr and for year 2021 was 28419.0 cr and for year 2022 was 34593.0 cr and for year 2023 was 41818.0 cr and for year 2024 was 37749.0 cr and for year 2025 was 35720.0 cr for company=TCS\n\nOperating Income for year 2016 was 28814.0 cr and for year 2017 was 30341.0 cr and for year 2018 was 30518.0 cr and for year 2019 was 37456.0 cr and for year 2020 was 38581.0 cr and for year 2021 was 42482.0 cr and for year 2022 was 48444.0 cr and for year 2023 was 54237.0 cr and for year 2024 was 59311.0 cr and for year 2025 was 62168.0 cr for company=TCS\n\nOperating Margin for year 2016 was 2650.0% and for year 2017 was 2570.0% and for year 2018 was 2480.0% and for year 2019 was 2560.0% and for year 2020 was 2460.0% and for year 2021 was 2590.0% and for year 2022 was 2530.0% and for year 2023 was 2410.0% and for year 2024 was 2460.0% and for year 2025 was 2430.0% for company=TCS\n\nInterest Expense, Total for year 2016 was -33.0 cr and for year 2017 was -32.0 cr and for year 2018 was -52.0 cr and for year 2019 was -198.0 cr and for year 2020 was -924.0 cr and for year 2021 was -637.0 cr and for year 2022 was -784.0 cr and for year 2023 was -779.0 cr and for year 2024 was -778.0 cr and for year 2025 was -796.0 cr for company=TCS\n\nInterest And Investment Income for year 2016 was 1756.0 cr and for year 2017 was 2264.0 cr and for year 2018 was 2454.0 cr and for year 2019 was 2780.0 cr and for year 2020 was 3572.0 cr and for year 2021 was 2512.0 cr and for year 2022 was 2667.0 cr and for year 2023 was 3263.0 cr and for year 2024 was 3822.0 cr and for year 2025 was 3339.0 cr for company=TCS\n\nNet Interest Expenses for year 2016 was 1723.0 cr and for year 2017 was 2232.0 cr and for year 2018 was 2402.0 cr and for year 2019 was 2582.0 cr and for year 2020 was 2648.0 cr and for year 2021 was 1875.0 cr and for year 2022 was 1883.0 cr and for year 2023 was 2484.0 cr and for year 2024 was 3044.0 cr and for year 2025 was 2543.0 cr for company=TCS\n\nCurrency Exchange Gains (Loss) for year 2016 was 742.0 cr and for year 2017 was 1240.0 cr and for year 2018 was 163.0 cr and for year 2019 was 967.0 cr and for year 2020 was 727.0 cr and for year 2021 was 248.0 cr and for year 2022 was 1045.0 cr and for year 2023 was -159.0 cr and for year 2024 was 223.0 cr and for year 2025 was 260.0 cr for company=TCS\n\nOther Non Operating Income (Expenses) for year 2016 was 91.0 cr and for year 2017 was 55.0 cr and for year 2018 was 78.0 cr and for year 2019 was 47.0 cr and for year 2020 was 19.0 cr and for year 2021 was 56.0 cr and for year 2022 was 87.0 cr and for year 2023 was 97.0 cr and for year 2024 was 51.0 cr and for year 2025 was 56.0 cr for company=TCS\n\nEBT, Excl. Unusual Items for year 2016 was 31370.0 cr and for year 2017 was 33868.0 cr and for year 2018 was 33161.0 cr and for year 2019 was 41052.0 cr and for year 2020 was 41975.0 cr and for year 2021 was 44661.0 cr and for year 2022 was 51459.0 cr and for year 2023 was 56659.0 cr and for year 2024 was 62629.0 cr and for year 2025 was 65027.0 cr for company=TCS\n\nGain (Loss) On Sale Of Investments for year 2016 was 465.0 cr and for year 2017 was 642.0 cr and for year 2018 was 906.0 cr and for year 2019 was 427.0 cr and for year 2020 was 214.0 cr and for year 2021 was 204.0 cr and for year 2022 was 198.0 cr and for year 2023 was 224.0 cr and for year 2024 was 312.0 cr and for year 2025 was 269.0 cr for company=TCS\n\nEBT, Incl. Unusual Items for year 2016 was 31840.0 cr and for year 2017 was 34513.0 cr and for year 2018 was 34092.0 cr and for year 2019 was 41563.0 cr and for year 2020 was 42248.0 cr and for year 2021 was 43760.0 cr and for year 2022 was 51687.0 cr and for year 2023 was 56907.0 cr and for year 2024 was 61997.0 cr and for year 2025 was 65331.0 cr for company=TCS\n\nIncome Tax Expense for year 2016 was 7502.0 cr and for year 2017 was 8156.0 cr and for year 2018 was 8212.0 cr and for year 2019 was 10001.0 cr and for year 2020 was 9801.0 cr and for year 2021 was 11198.0 cr and for year 2022 was 13238.0 cr and for year 2023 was 14604.0 cr and for year 2024 was 15898.0 cr and for year 2025 was 16534.0 cr for company=TCS\n\nEarnings From Continuing Operations for year 2016 was 24338.0 cr and for year 2017 was 26357.0 cr and for year 2018 was 25880.0 cr and for year 2019 was 31562.0 cr and for year 2020 was 32447.0 cr and for year 2021 was 32562.0 cr and for year 2022 was 38449.0 cr and for year 2023 was 42303.0 cr and for year 2024 was 46099.0 cr and for year 2025 was 48797.0 cr for company=TCS\n\nMinority Interest for year 2016 was -68.0 cr and for year 2017 was -68.0 cr and for year 2018 was -54.0 cr and for year 2019 was -90.0 cr and for year 2020 was -107.0 cr and for year 2021 was -132.0 cr and for year 2022 was -122.0 cr and for year 2023 was -156.0 cr and for year 2024 was -191.0 cr and for year 2025 was -244.0 cr for company=TCS\n\nNet Income for year 2016 was 24270.0 cr and for year 2017 was 26289.0 cr and for year 2018 was 25826.0 cr and for year 2019 was 31472.0 cr and for year 2020 was 32340.0 cr and for year 2021 was 32430.0 cr and for year 2022 was 38327.0 cr and for year 2023 was 42147.0 cr and for year 2024 was 45908.0 cr and for year 2025 was 48553.0 cr for company=TCS\n\nNet Income to Common Incl Extra Items for year 2016 was 24270.0 cr and for year 2017 was 26289.0 cr and for year 2018 was 25826.0 cr and for year 2019 was 31472.0 cr and for year 2020 was 32340.0 cr and for year 2021 was 32430.0 cr and for year 2022 was 38327.0 cr and for year 2023 was 42147.0 cr and for year 2024 was 45908.0 cr and for year 2025 was 48553.0 cr for company=TCS\n\nNet Income to Common Excl. Extra Items for year 2016 was 24270.0 cr and for year 2017 was 26289.0 cr and for year 2018 was 25826.0 cr and for year 2019 was 31472.0 cr and for year 2020 was 32340.0 cr and for year 2021 was 32430.0 cr and for year 2022 was 38327.0 cr and for year 2023 was 42147.0 cr and for year 2024 was 45908.0 cr and for year 2025 was 48553.0 cr for company=TCS\n\nTotal Shares Outstanding for year 2016 was 394.09000000000003 cr and for year 2017 was 394.09000000000003 cr and for year 2018 was 382.86 cr and for year 2019 was 375.24 cr and for year 2020 was 375.24 cr and for year 2021 was 369.90999999999997 cr and for year 2022 was 365.90999999999997 cr and for year 2023 was 365.90999999999997 cr and for year 2024 was 361.81 cr and for year 2025 was 361.81 cr for company=TCS\n\nWeighted Avg. Shares Outstanding for year 2016 was 394.09000000000003 cr and for year 2017 was 394.09000000000003 cr and for year 2018 was 384.91999999999996 cr and for year 2019 was 378.96999999999997 cr and for year 2020 was 375.24 cr and for year 2021 was 374.01 cr and for year 2022 was 369.88 cr and for year 2023 was 365.90999999999997 cr and for year 2024 was 364.69 cr and for year 2025 was 361.81 cr for company=TCS\n\nWeighted Avg. Shares Outstanding Dil for year 2016 was 394.09000000000003 cr and for year 2017 was 394.09000000000003 cr and for year 2018 was 384.91999999999996 cr and for year 2019 was 378.96999999999997 cr and for year 2020 was 375.24 cr and for year 2021 was 374.01 cr and for year 2022 was 369.88 cr and for year 2023 was 365.90999999999997 cr and for year 2024 was 364.69 cr and for year 2025 was 361.81 cr for company=TCS\n\nEPS for year 2016 was 6.16 cr and for year 2017 was 6.67 cr and for year 2018 was 6.709999999999999 cr and for year 2019 was 8.3 cr and for year 2020 was 8.620000000000001 cr and for year 2021 was 8.67 cr and for year 2022 was 10.36 cr and for year 2023 was 11.52 cr and for year 2024 was 12.59 cr and for year 2025 was 13.419999999999998 cr for company=TCS\n\nEBITDA for year 2016 was 30702.0 cr and for year 2017 was 32328.0 cr and for year 2018 was 32532.0 cr and for year 2019 was 39512.0 cr and for year 2020 was 40842.0 cr and for year 2021 was 45047.0 cr and for year 2022 was 51488.0 cr and for year 2023 was 57625.0 cr and for year 2024 was 62580.0 cr and for year 2025 was 65590.0 cr for company=TCS\n\nEffective Tax Rate for year 2016 was 2360.0% and for year 2017 was 2360.0% and for year 2018 was 2410.0% and for year 2019 was 2410.0% and for year 2020 was 2320.0% and for year 2021 was 2560.0% and for year 2022 was 2560.0% and for year 2023 was 2570.0% and for year 2024 was 2560.0% and for year 2025 was 2530.0% for company=TCS\n\n--- Data for JIOFIN ---\nOther Revenues, Total for year 2016 was 0.42000000000000004 cr and for year 2017 was 702.17 cr and for year 2018 was -50.5 cr and for year 2019 was -46.88 cr and for year 2020 was 67.75 cr and for year 2021 was N/A and for year 2022 was 35.4 cr and for year 2023 was 10.059999999999999 cr and for year 2024 was -2.05 cr and for year 2025 was 753.92 cr for company=JIOFIN\n\nTotal Revenues for year 2016 was -123.69000000000001 cr and for year 2017 was 435.61 cr and for year 2018 was 225.08 cr and for year 2019 was -352.91999999999996 cr and for year 2020 was 221.49 cr and for year 2021 was 184.60999999999999 cr and for year 2022 was 184.01 cr and for year 2023 was 51.69 cr and for year 2024 was 1841.56 cr and for year 2025 was 1994.9099999999999 cr for company=JIOFIN\n\nTotal Revenues % Chg. for year 2016 was N/A and for year 2017 was N/A and for year 2018 was -4830.0% and for year 2019 was N/A and for year 2020 was N/A and for year 2021 was -1670.0% and for year 2022 was -30.0% and for year 2023 was -7190.000000000001% and for year 2024 was 3460.0% and for year 2025 was 830.0000000000001% for company=JIOFIN\n\nCost of Goods Sold, Total for year 2016 was 164.85999999999999 cr and for year 2017 was 0.32 cr and for year 2018 was 0.32 cr and for year 2019 was 0.42000000000000004 cr and for year 2020 was N/A and for year 2021 was N/A and for year 2022 was N/A and for year 2023 was N/A and for year 2024 was N/A and for year 2025 was N/A for company=JIOFIN\n\nGross Profit for year 2016 was -288.55 cr and for year 2017 was 435.28999999999996 cr and for year 2018 was 224.76 cr and for year 2019 was -353.34000000000003 cr and for year 2020 was 221.49 cr and for year 2021 was 184.60999999999999 cr and for year 2022 was 184.01 cr and for year 2023 was 51.69 cr and for year 2024 was 1841.56 cr and for year 2025 was 1994.9099999999999 cr for company=JIOFIN\n\nGross Profit Margin for year 2016 was 23.330000000000002 cr and for year 2017 was 9.99 cr and for year 2018 was 9.99 cr and for year 2019 was 10.01 cr and for year 2020 was 10.0 cr and for year 2021 was 10.0 cr and for year 2022 was 10.0 cr and for year 2023 was 10.0 cr and for year 2024 was 10.0 cr and for year 2025 was 10.0 cr for company=JIOFIN\n\nSelling General & Admin Expenses, Total for year 2016 was 0.15 cr and for year 2017 was 0.64 cr and for year 2018 was 1.1400000000000001 cr and for year 2019 was 1.3800000000000001 cr and for year 2020 was N/A and for year 2021 was 69.78999999999999 cr and for year 2022 was 7.040000000000001 cr and for year 2023 was N/A and for year 2024 was 116.04 cr and for year 2025 was 214.92 cr for company=JIOFIN\n\nDepreciation & Amortization for year 2016 was N/A and for year 2017 was N/A and for year 2018 was N/A and for year 2019 was N/A and for year 2020 was N/A and for year 2021 was N/A and for year 2022 was N/A and for year 2023 was N/A and for year 2024 was 21.52 cr and for year 2025 was 22.52 cr for company=JIOFIN\n\nOther Operating Expenses for year 2016 was 5.51 cr and for year 2017 was 9.72 cr and for year 2018 was 10.23 cr and for year 2019 was 9.15 cr and for year 2020 was 27.65 cr and for year 2021 was -7.840000000000001 cr and for year 2022 was N/A and for year 2023 was 5.5600000000000005 cr and for year 2024 was 177.43 cr and for year 2025 was 239.39000000000001 cr for company=JIOFIN\n\nOther Operating Expenses, Total for year 2016 was 5.66 cr and for year 2017 was 10.36 cr and for year 2018 was 11.370000000000001 cr and for year 2019 was 10.53 cr and for year 2020 was 27.65 cr and for year 2021 was 61.95 cr and for year 2022 was 7.040000000000001 cr and for year 2023 was 5.5600000000000005 cr and for year 2024 was 314.99 cr and for year 2025 was 476.83000000000004 cr for company=JIOFIN\n\nOperating Income for year 2016 was -294.21 cr and for year 2017 was 424.93 cr and for year 2018 was 213.39000000000001 cr and for year 2019 was -363.87 cr and for year 2020 was 193.84 cr and for year 2021 was 122.66 cr and for year 2022 was 176.97 cr and for year 2023 was 46.13 cr and for year 2024 was 1526.5700000000002 cr and for year 2025 was 1518.08 cr for company=JIOFIN\n\nIncome (Loss) On Equity Invest. for year 2016 was N/A and for year 2017 was N/A and for year 2018 was N/A and for year 2019 was N/A and for year 2020 was N/A and for year 2021 was N/A and for year 2022 was N/A and for year 2023 was N/A and for year 2024 was 428.52 cr and for year 2025 was 392.82 cr for company=JIOFIN\n\nOther Non Operating Income (Expenses) for year 2016 was -0.65 cr and for year 2017 was -0.73 cr and for year 2018 was -3.2 cr and for year 2019 was -5.0 cr and for year 2020 was -1.0 cr and for year 2021 was 7.65 cr and for year 2022 was -10.0 cr and for year 2023 was 3.21 cr and for year 2024 was 0.8 cr and for year 2025 was 36.010000000000005 cr for company=JIOFIN\n\nEBT, Excl. Unusual Items for year 2016 was -294.86 cr and for year 2017 was 424.2 cr and for year 2018 was 210.19 cr and for year 2019 was -368.87 cr and for year 2020 was 193.82999999999998 cr and for year 2021 was 130.31 cr and for year 2022 was 176.87 cr and for year 2023 was 49.339999999999996 cr and for year 2024 was 1955.89 cr and for year 2025 was 1946.9099999999999 cr for company=JIOFIN\n\nGain (Loss) On Sale Of Investments for year 2016 was N/A and for year 2017 was N/A and for year 2018 was N/A and for year 2019 was N/A and for year 2020 was N/A and for year 2021 was -12.25 cr and for year 2022 was N/A and for year 2023 was N/A and for year 2024 was N/A and for year 2025 was N/A for company=JIOFIN\n\nEBT, Incl. Unusual Items for year 2016 was -294.86 cr and for year 2017 was 424.2 cr and for year 2018 was 210.19 cr and for year 2019 was -368.87 cr and for year 2020 was 193.82999999999998 cr and for year 2021 was 118.05999999999999 cr and for year 2022 was 176.87 cr and for year 2023 was 49.339999999999996 cr and for year 2024 was 1955.89 cr and for year 2025 was 1946.9099999999999 cr for company=JIOFIN\n\nIncome Tax Expense for year 2016 was 5.970000000000001 cr and for year 2017 was 94.22999999999999 cr and for year 2018 was -31.5 cr and for year 2019 was 32.81 cr and for year 2020 was -36.11 cr and for year 2021 was -5.0 cr and for year 2022 was 8.83 cr and for year 2023 was 18.09 cr and for year 2024 was 351.34000000000003 cr and for year 2025 was 334.32 cr for company=JIOFIN\n\nEarnings From Continuing Operations for year 2016 was -300.83000000000004 cr and for year 2017 was 329.96999999999997 cr and for year 2018 was 241.69 cr and for year 2019 was -401.68 cr and for year 2020 was 229.94 cr and for year 2021 was 123.05999999999999 cr and for year 2022 was 168.04000000000002 cr and for year 2023 was 31.25 cr and for year 2024 was 1604.55 cr and for year 2025 was 1612.59 cr for company=JIOFIN\n\nNet Income for year 2016 was -300.83000000000004 cr and for year 2017 was 329.96999999999997 cr and for year 2018 was 241.69 cr and for year 2019 was -401.68 cr and for year 2020 was 229.94 cr and for year 2021 was 123.05999999999999 cr and for year 2022 was 168.04000000000002 cr and for year 2023 was 31.25 cr and for year 2024 was 1604.55 cr and for year 2025 was 1612.59 cr for company=JIOFIN\n\nNet Income to Common Incl Extra Items for year 2016 was -300.83000000000004 cr and for year 2017 was 329.96999999999997 cr and for year 2018 was 241.69 cr and for year 2019 was -401.68 cr and for year 2020 was 229.94 cr and for year 2021 was 123.05999999999999 cr and for year 2022 was 168.04000000000002 cr and for year 2023 was 31.25 cr and for year 2024 was 1604.55 cr and for year 2025 was 1612.59 cr for company=JIOFIN\n\nNet Income to Common Excl. Extra Items for year 2016 was -300.83000000000004 cr and for year 2017 was 329.96999999999997 cr and for year 2018 was 241.69 cr and for year 2019 was -401.68 cr and for year 2020 was 229.94 cr and for year 2021 was 123.05999999999999 cr and for year 2022 was 168.04000000000002 cr and for year 2023 was 31.25 cr and for year 2024 was 1604.55 cr and for year 2025 was 1612.59 cr for company=JIOFIN\n\nTotal Shares Outstanding for year 2016 was 0.2 cr and for year 2017 was 0.2 cr and for year 2018 was 0.2 cr and for year 2019 was 0.2 cr and for year 2020 was 0.2 cr and for year 2021 was 0.2 cr and for year 2022 was 0.2 cr and for year 2023 was 0.2 cr and for year 2024 was 635.33 cr and for year 2025 was 635.3100000000001 cr for company=JIOFIN\n\nWeighted Avg. Shares Outstanding for year 2016 was 0.2 cr and for year 2017 was 0.2 cr and for year 2018 was 0.2 cr and for year 2019 was 0.2 cr and for year 2020 was 0.52 cr and for year 2021 was 0.52 cr and for year 2022 was 0.52 cr and for year 2023 was 0.52 cr and for year 2024 was 635.33 cr and for year 2025 was 634.88 cr for company=JIOFIN\n\nWeighted Avg. Shares Outstanding Dil for year 2016 was 0.2 cr and for year 2017 was 0.52 cr and for year 2018 was 0.52 cr and for year 2019 was 0.2 cr and for year 2020 was 0.52 cr and for year 2021 was 0.52 cr and for year 2022 was 0.52 cr and for year 2023 was 0.52 cr and for year 2024 was 635.33 cr and for year 2025 was 634.88 cr for company=JIOFIN\n\nEPS for year 2016 was -148.91 cr and for year 2017 was 163.34 cr and for year 2018 was 119.64000000000001 cr and for year 2019 was -198.82999999999998 cr and for year 2020 was 44.489999999999995 cr and for year 2021 was 23.81 cr and for year 2022 was 32.510000000000005 cr and for year 2023 was 6.05 cr and for year 2024 was 0.25 cr and for year 2025 was 0.25 cr for company=JIOFIN\n\nEPS Diluted for year 2016 was -148.91 cr and for year 2017 was 63.839999999999996 cr and for year 2018 was 46.760000000000005 cr and for year 2019 was -198.82999999999998 cr and for year 2020 was 44.489999999999995 cr and for year 2021 was 23.81 cr and for year 2022 was 32.510000000000005 cr and for year 2023 was 6.05 cr and for year 2024 was 0.25 cr and for year 2025 was 0.25 cr for company=JIOFIN\n\nEffective Tax Rate for year 2016 was -200.0% and for year 2017 was 2220.0% and for year 2018 was -1500.0% and for year 2019 was -890.0% and for year 2020 was -1860.0000000000002% and for year 2021 was -420.0% and for year 2022 was 500.0% and for year 2023 was 3670.0000000000005% and for year 2024 was 1800.0% and for year 2025 was 1720.0% for company=JIOFIN\n\nOperating Revenues for year 2016 was N/A and for year 2017 was N/A and for year 2018 was N/A and for year 2019 was N/A and for year 2020 was N/A and for year 2021 was N/A and for year 2022 was N/A and for year 2023 was N/A and for year 2024 was 151.66 cr and for year 2025 was 155.17000000000002 cr for company=JIOFIN\n\nGain (Loss) on Sale of Investment, Total for year 2016 was -12.290000000000001 cr and for year 2017 was N/A and for year 2018 was 289.86 cr and for year 2019 was -169.92000000000002 cr and for year 2020 was -15.38 cr and for year 2021 was N/A and for year 2022 was 0.52 cr and for year 2023 was 3.02 cr and for year 2024 was 547.63 cr and for year 2025 was N/A for company=JIOFIN\n\nInterest And Invest. Income for year 2016 was -111.82000000000001 cr and for year 2017 was -266.56 cr and for year 2018 was -14.280000000000001 cr and for year 2019 was -136.12 cr and for year 2020 was 169.12 cr and for year 2021 was 184.60999999999999 cr and for year 2022 was 148.09 cr and for year 2023 was 38.61 cr and for year 2024 was 1144.3200000000002 cr and for year 2025 was 1085.8200000000002 cr for company=JIOFIN\n\n--- Balance Sheet Data ---\n\n--- Data for TCS ---\nLong-Term Leases for year 2016 was 83.0 cr and for year 2017 was 71.0 cr and for year 2018 was 54.0 cr and for year 2019 was 44.0 cr and for year 2020 was 6906.0 cr and for year 2021 was 6503.0 cr and for year 2022 was 6368.0 cr and for year 2023 was 6203.0 cr and for year 2024 was 6516.0 cr and for year 2025 was 7838.0 cr for company=TCS\n\nAccumulated Depreciation for year 2016 was -9337.0 cr and for year 2017 was -10834.0 cr and for year 2018 was -12504.0 cr and for year 2019 was -14111.0 cr and for year 2020 was -16771.0 cr and for year 2021 was -17548.0 cr and for year 2022 was -19526.0 cr and for year 2023 was -22114.0 cr and for year 2024 was -24477.0 cr and for year 2025 was -26299.0 cr for company=TCS\n\nAdditional Paid In Capital for year 2016 was 1919.0 cr and for year 2017 was 1919.0 cr and for year 2018 was N/A and for year 2019 was N/A and for year 2020 was N/A and for year 2021 was N/A and for year 2022 was N/A and for year 2023 was N/A and for year 2024 was N/A and for year 2025 was N/A for company=TCS\n\nCash And Equivalents for year 2016 was 6295.0 cr and for year 2017 was 3597.0 cr and for year 2018 was 4883.0 cr and for year 2019 was 7224.0 cr and for year 2020 was 8646.0 cr and for year 2021 was 6858.0 cr and for year 2022 was 12488.0 cr and for year 2023 was 7123.0 cr and for year 2024 was 9016.0 cr and for year 2025 was 8342.0 cr for company=TCS\n\nAccounts Payable, Total for year 2016 was 7541.0 cr and for year 2017 was 4905.0 cr and for year 2018 was 5094.0 cr and for year 2019 was 6292.0 cr and for year 2020 was 6740.0 cr and for year 2021 was 2237.0 cr and for year 2022 was 8045.0 cr and for year 2023 was 10515.0 cr and for year 2024 was 3236.0 cr and for year 2025 was 7657.0 cr for company=TCS\n\nAccounts Receivable Long-Term for year 2016 was N/A and for year 2017 was 210.0 cr and for year 2018 was 321.0 cr and for year 2019 was 676.0 cr and for year 2020 was 595.0 cr and for year 2021 was 578.0 cr and for year 2022 was 371.0 cr and for year 2023 was 563.0 cr and for year 2024 was 438.0 cr and for year 2025 was 424.0 cr for company=TCS\n\nAccounts Receivable, Total for year 2016 was 28065.0 cr and for year 2017 was 27825.0 cr and for year 2018 was 31629.0 cr and for year 2019 was 35741.0 cr and for year 2020 was 40556.0 cr and for year 2021 was 40492.0 cr and for year 2022 was 46058.0 cr and for year 2023 was 55570.0 cr and for year 2024 was 59423.0 cr and for year 2025 was 65279.0 cr for company=TCS\n\nAccrued Expenses, Total for year 2016 was 3096.0 cr and for year 2017 was 4611.0 cr and for year 2018 was 6740.0 cr and for year 2019 was 8145.0 cr and for year 2020 was 9532.0 cr and for year 2021 was 17329.0 cr and for year 2022 was 17206.0 cr and for year 2023 was 15031.0 cr and for year 2024 was 21354.0 cr and for year 2025 was 21410.0 cr for company=TCS\n\nCommon Stock, Total for year 2016 was 197.0 cr and for year 2017 was 197.0 cr and for year 2018 was 191.0 cr and for year 2019 was 375.0 cr and for year 2020 was 375.0 cr and for year 2021 was 370.0 cr and for year 2022 was 366.0 cr and for year 2023 was 366.0 cr and for year 2024 was 362.0 cr and for year 2025 was 362.0 cr for company=TCS\n\nComprehensive Income and Other for year 2016 was 2109.0 cr and for year 2017 was 2163.0 cr and for year 2018 was 1923.0 cr and for year 2019 was 2182.0 cr and for year 2020 was 2945.0 cr and for year 2021 was 3505.0 cr and for year 2022 was 3166.0 cr and for year 2023 was 3384.0 cr and for year 2024 was 3700.0 cr and for year 2025 was 4359.0 cr for company=TCS\n\nCurrent Income Taxes Payable for year 2016 was 805.0 cr and for year 2017 was 1412.0 cr and for year 2018 was 1421.0 cr and for year 2019 was 2667.0 cr and for year 2020 was 3712.0 cr and for year 2021 was 6243.0 cr and for year 2022 was 7921.0 cr and for year 2023 was 9345.0 cr and for year 2024 was 11433.0 cr and for year 2025 was 12715.0 cr for company=TCS\n\nCurrent Portion of Leases for year 2016 was 49.0 cr and for year 2017 was 18.0 cr and for year 2018 was 12.0 cr and for year 2019 was 18.0 cr and for year 2020 was 1268.0 cr and for year 2021 was 1292.0 cr and for year 2022 was 1450.0 cr and for year 2023 was 1485.0 cr and for year 2024 was 1505.0 cr and for year 2025 was 1554.0 cr for company=TCS\n\nDeferred Charges Long-Term for year 2016 was N/A and for year 2017 was N/A and for year 2018 was 95.0 cr and for year 2019 was 174.0 cr and for year 2020 was 286.0 cr and for year 2021 was 228.0 cr and for year 2022 was 150.0 cr and for year 2023 was 114.0 cr and for year 2024 was 247.0 cr and for year 2025 was 297.0 cr for company=TCS\n\nDeferred Tax Assets Long-Term for year 2016 was 2908.0 cr and for year 2017 was 2828.0 cr and for year 2018 was 3449.0 cr and for year 2019 was 2656.0 cr and for year 2020 was 2828.0 cr and for year 2021 was 3931.0 cr and for year 2022 was 3708.0 cr and for year 2023 was 3307.0 cr and for year 2024 was 3403.0 cr and for year 2025 was 3578.0 cr for company=TCS\n\nDeferred Tax Liability Non Current for year 2016 was 805.0 cr and for year 2017 was 919.0 cr and for year 2018 was 1170.0 cr and for year 2019 was 1042.0 cr and for year 2020 was 779.0 cr and for year 2021 was 767.0 cr and for year 2022 was 590.0 cr and for year 2023 was 792.0 cr and for year 2024 was 977.0 cr and for year 2025 was 980.0 cr for company=TCS\n\nGoodwill for year 2016 was 1669.0 cr and for year 2017 was 1597.0 cr and for year 2018 was 1745.0 cr and for year 2019 was 1700.0 cr and for year 2020 was 1710.0 cr and for year 2021 was 1798.0 cr and for year 2022 was 1787.0 cr and for year 2023 was 1858.0 cr and for year 2024 was 1832.0 cr and for year 2025 was 1860.0 cr for company=TCS\n\nGross Property Plant And Equipment for year 2016 was 20978.0 cr and for year 2017 was 22432.0 cr and for year 2018 was 23998.0 cr and for year 2019 was 25485.0 cr and for year 2020 was 36612.0 cr and for year 2021 was 37217.0 cr and for year 2022 was 39141.0 cr and for year 2023 was 41138.0 cr and for year 2024 was 43303.0 cr and for year 2025 was 48098.0 cr for company=TCS\n\nInventory for year 2016 was 16.0 cr and for year 2017 was 21.0 cr and for year 2018 was 26.0 cr and for year 2019 was 10.0 cr and for year 2020 was 5.0 cr and for year 2021 was 8.0 cr and for year 2022 was 20.0 cr and for year 2023 was 28.0 cr and for year 2024 was 28.0 cr and for year 2025 was 21.0 cr for company=TCS\n\nLoans Receivable Long-Term for year 2016 was 2472.0 cr and for year 2017 was 9.0 cr and for year 2018 was 1975.0 cr and for year 2019 was 60.0 cr and for year 2020 was 29.0 cr and for year 2021 was 29.0 cr and for year 2022 was 311.0 cr and for year 2023 was 173.0 cr and for year 2024 was 2.0 cr and for year 2025 was 25.0 cr for company=TCS\n\nLong-term Investments for year 2016 was 758.0 cr and for year 2017 was 204.0 cr and for year 2018 was 116.0 cr and for year 2019 was 58.0 cr and for year 2020 was 390.0 cr and for year 2021 was 757.0 cr and for year 2022 was 1268.0 cr and for year 2023 was 1371.0 cr and for year 2024 was 2280.0 cr and for year 2025 was 1717.0 cr for company=TCS\n\nMinority Interest for year 2016 was 355.0 cr and for year 2017 was 366.0 cr and for year 2018 was 402.0 cr and for year 2019 was 453.0 cr and for year 2020 was 623.0 cr and for year 2021 was 675.0 cr and for year 2022 was 707.0 cr and for year 2023 was 782.0 cr and for year 2024 was 830.0 cr and for year 2025 was 1015.0 cr for company=TCS\n\nNet Property Plant And Equipment for year 2016 was 11641.0 cr and for year 2017 was 11598.0 cr and for year 2018 was 11494.0 cr and for year 2019 was 11374.0 cr and for year 2020 was 19841.0 cr and for year 2021 was 19669.0 cr and for year 2022 was 19615.0 cr and for year 2023 was 19024.0 cr and for year 2024 was 18826.0 cr and for year 2025 was 21799.0 cr for company=TCS\n\nNotes Receivable for year 2016 was 2743.0 cr and for year 2017 was 2909.0 cr and for year 2018 was 3205.0 cr and for year 2019 was 8029.0 cr and for year 2020 was 8475.0 cr and for year 2021 was 11472.0 cr and for year 2022 was 6445.0 cr and for year 2023 was 1325.0 cr and for year 2024 was 491.0 cr and for year 2025 was 9.0 cr for company=TCS\n\nOther Current Assets, Total for year 2016 was 1168.0 cr and for year 2017 was 1498.0 cr and for year 2018 was 1131.0 cr and for year 2019 was 2012.0 cr and for year 2020 was 1851.0 cr and for year 2021 was 3831.0 cr and for year 2022 was 5983.0 cr and for year 2023 was 4530.0 cr and for year 2024 was 4730.0 cr and for year 2025 was 10585.0 cr for company=TCS\n\nOther Current Liabilities for year 2016 was 2606.0 cr and for year 2017 was 1968.0 cr and for year 2018 was 2348.0 cr and for year 2019 was 1675.0 cr and for year 2020 was 2086.0 cr and for year 2021 was 2490.0 cr and for year 2022 was 3060.0 cr and for year 2023 was 2202.0 cr and for year 2024 was 3427.0 cr and for year 2025 was 4323.0 cr for company=TCS\n\nOther Intangibles, Total for year 2016 was 134.0 cr and for year 2017 was 47.0 cr and for year 2018 was 12.0 cr and for year 2019 was 179.0 cr and for year 2020 was 283.0 cr and for year 2021 was 480.0 cr and for year 2022 was 1101.0 cr and for year 2023 was 867.0 cr and for year 2024 was 510.0 cr and for year 2025 was 940.0 cr for company=TCS\n\nOther Long-Term Assets, Total for year 2016 was 6301.0 cr and for year 2017 was 6443.0 cr and for year 2018 was 5865.0 cr and for year 2019 was 5935.0 cr and for year 2020 was 4700.0 cr and for year 2021 was 4009.0 cr and for year 2022 was 4893.0 cr and for year 2023 was 6104.0 cr and for year 2024 was 5927.0 cr and for year 2025 was 5978.0 cr for company=TCS\n\nOther Non Current Liabilities for year 2016 was 975.0 cr and for year 2017 was 925.0 cr and for year 2018 was 921.0 cr and for year 2019 was 700.0 cr and for year 2020 was 291.0 cr and for year 2021 was 280.0 cr and for year 2022 was 572.0 cr and for year 2023 was 353.0 cr and for year 2024 was 365.0 cr and for year 2025 was 364.0 cr for company=TCS\n\nOther Receivables for year 2016 was 578.0 cr and for year 2017 was 1229.0 cr and for year 2018 was 1270.0 cr and for year 2019 was 3857.0 cr and for year 2020 was 2126.0 cr and for year 2021 was 2125.0 cr and for year 2022 was 1969.0 cr and for year 2023 was 1777.0 cr and for year 2024 was 2203.0 cr and for year 2025 was 2956.0 cr for company=TCS\n\nPension & Other Post Retirement Benefits for year 2016 was 237.0 cr and for year 2017 was 245.0 cr and for year 2018 was 290.0 cr and for year 2019 was 330.0 cr and for year 2020 was 417.0 cr and for year 2021 was 749.0 cr and for year 2022 was 677.0 cr and for year 2023 was 536.0 cr and for year 2024 was 686.0 cr and for year 2025 was 841.0 cr for company=TCS\n\nPrepaid Expenses for year 2016 was 1376.0 cr and for year 2017 was 1509.0 cr and for year 2018 was 1137.0 cr and for year 2019 was 664.0 cr and for year 2020 was 1513.0 cr and for year 2021 was 4679.0 cr and for year 2022 was 3012.0 cr and for year 2023 was 1514.0 cr and for year 2024 was 2055.0 cr and for year 2025 was 2383.0 cr for company=TCS\n\nRestricted Cash for year 2016 was 440.0 cr and for year 2017 was 122.0 cr and for year 2018 was 222.0 cr and for year 2019 was 196.0 cr and for year 2020 was 215.0 cr and for year 2021 was 209.0 cr and for year 2022 was 226.0 cr and for year 2023 was N/A and for year 2024 was 471.0 cr and for year 2025 was 906.0 cr for company=TCS\n\nRetained Earnings for year 2016 was 66847.0 cr and for year 2017 was 81935.0 cr and for year 2018 was 83014.0 cr and for year 2019 was 86889.0 cr and for year 2020 was 80806.0 cr and for year 2021 was 82558.0 cr and for year 2022 was 85607.0 cr and for year 2023 was 86674.0 cr and for year 2024 was 86427.0 cr and for year 2025 was 90035.0 cr for company=TCS\n\nShort Term Investments for year 2016 was 22532.0 cr and for year 2017 was 41606.0 cr and for year 2018 was 37721.0 cr and for year 2019 was 34398.0 cr and for year 2020 was 26850.0 cr and for year 2021 was 29606.0 cr and for year 2022 was 32109.0 cr and for year 2023 was 38403.0 cr and for year 2024 was 34567.0 cr and for year 2025 was 32530.0 cr for company=TCS\n\nShort-term Borrowings for year 2016 was 113.0 cr and for year 2017 was 200.0 cr and for year 2018 was 181.0 cr and for year 2019 was N/A and for year 2020 was N/A and for year 2021 was N/A and for year 2022 was N/A and for year 2023 was N/A and for year 2024 was N/A and for year 2025 was N/A for company=TCS\n\nTotal Assets for year 2016 was 89096.0 cr and for year 2017 was 103252.0 cr and for year 2018 was 106296.0 cr and for year 2019 was 114943.0 cr and for year 2020 was 120899.0 cr and for year 2021 was 130759.0 cr and for year 2022 was 141514.0 cr and for year 2023 was 143651.0 cr and for year 2024 was 146449.0 cr and for year 2025 was 159629.0 cr for company=TCS\n\nTotal Cash And Short Term Investments for year 2016 was 28827.0 cr and for year 2017 was 45203.0 cr and for year 2018 was 42604.0 cr and for year 2019 was 41622.0 cr and for year 2020 was 35496.0 cr and for year 2021 was 36464.0 cr and for year 2022 was 44597.0 cr and for year 2023 was 45526.0 cr and for year 2024 was 43583.0 cr and for year 2025 was 40872.0 cr for company=TCS\n\nTotal Common Equity for year 2016 was 71072.0 cr and for year 2017 was 86214.0 cr and for year 2018 was 85128.0 cr and for year 2019 was 89446.0 cr and for year 2020 was 84126.0 cr and for year 2021 was 86433.0 cr and for year 2022 was 89139.0 cr and for year 2023 was 90424.0 cr and for year 2024 was 90489.0 cr and for year 2025 was 94756.0 cr for company=TCS\n\nTotal Current Assets for year 2016 was 63213.0 cr and for year 2017 was 80316.0 cr and for year 2018 was 81224.0 cr and for year 2019 was 92131.0 cr and for year 2020 was 90237.0 cr and for year 2021 was 99280.0 cr and for year 2022 was 108310.0 cr and for year 2023 was 110270.0 cr and for year 2024 was 112984.0 cr and for year 2025 was 123011.0 cr for company=TCS\n\nTotal Current Liabilities for year 2016 was 15569.0 cr and for year 2017 was 14512.0 cr and for year 2018 was 17828.0 cr and for year 2019 was 22084.0 cr and for year 2020 was 27060.0 cr and for year 2021 was 34155.0 cr and for year 2022 was 42351.0 cr and for year 2023 was 43558.0 cr and for year 2024 was 46104.0 cr and for year 2025 was 53001.0 cr for company=TCS\n\nTotal Equity for year 2016 was 71427.0 cr and for year 2017 was 86580.0 cr and for year 2018 was 85530.0 cr and for year 2019 was 89899.0 cr and for year 2020 was 84749.0 cr and for year 2021 was 87108.0 cr and for year 2022 was 89846.0 cr and for year 2023 was 91206.0 cr and for year 2024 was 91319.0 cr and for year 2025 was 95771.0 cr for company=TCS\n\nTotal Liabilities for year 2016 was 17669.0 cr and for year 2017 was 16672.0 cr and for year 2018 was 20766.0 cr and for year 2019 was 25044.0 cr and for year 2020 was 36150.0 cr and for year 2021 was 43651.0 cr and for year 2022 was 51668.0 cr and for year 2023 was 52445.0 cr and for year 2024 was 55130.0 cr and for year 2025 was 63858.0 cr for company=TCS\n\nTotal Liabilities And Equity for year 2016 was 89096.0 cr and for year 2017 was 103252.0 cr and for year 2018 was 106296.0 cr and for year 2019 was 114943.0 cr and for year 2020 was 120899.0 cr and for year 2021 was 130759.0 cr and for year 2022 was 141514.0 cr and for year 2023 was 143651.0 cr and for year 2024 was 146449.0 cr and for year 2025 was 159629.0 cr for company=TCS\n\nTotal Receivables for year 2016 was 31386.0 cr and for year 2017 was 31963.0 cr and for year 2018 was 36104.0 cr and for year 2019 was 47627.0 cr and for year 2020 was 51157.0 cr and for year 2021 was 54089.0 cr and for year 2022 was 54472.0 cr and for year 2023 was 58672.0 cr and for year 2024 was 62117.0 cr and for year 2025 was 68244.0 cr for company=TCS\n\nUnearned Revenue Current, Total for year 2016 was 1359.0 cr and for year 2017 was 1398.0 cr and for year 2018 was 2032.0 cr and for year 2019 was 3287.0 cr and for year 2020 was 3722.0 cr and for year 2021 was 4564.0 cr and for year 2022 was 4669.0 cr and for year 2023 was 4980.0 cr and for year 2024 was 5149.0 cr and for year 2025 was 5342.0 cr for company=TCS\n\nUnearned Revenue Non Current for year 2016 was N/A and for year 2017 was N/A and for year 2018 was 503.0 cr and for year 2019 was 844.0 cr and for year 2020 was 697.0 cr and for year 2021 was 1197.0 cr and for year 2022 was 1110.0 cr and for year 2023 was 1003.0 cr and for year 2024 was 482.0 cr and for year 2025 was 834.0 cr for company=TCS\n\n--- Data for JIOFIN ---\nAccounts Payable, Total for year 2016 was 74.01 cr and for year 2017 was N/A and for year 2018 was N/A and for year 2019 was 0.86 cr and for year 2020 was 0.04 cr and for year 2021 was N/A and for year 2022 was N/A and for year 2023 was 16.380000000000003 cr and for year 2024 was 16.27 cr and for year 2025 was 33.3 cr for company=JIOFIN\n\nAccounts Receivable, Total for year 2016 was 242.18 cr and for year 2017 was 165.45999999999998 cr and for year 2018 was 31.54 cr and for year 2019 was 7.65 cr and for year 2020 was 4382.5 cr and for year 2021 was 1951.0900000000001 cr and for year 2022 was 2001.0 cr and for year 2023 was 41.089999999999996 cr and for year 2024 was 173.31 cr and for year 2025 was 10053.119999999999 cr for company=JIOFIN\n\nAccrued Expenses, Total for year 2016 was 258.65 cr and for year 2017 was 239.91 cr and for year 2018 was N/A and for year 2019 was N/A and for year 2020 was 58.96 cr and for year 2021 was 0.06999999999999999 cr and for year 2022 was 0.06999999999999999 cr and for year 2023 was 42.1 cr and for year 2024 was 131.14000000000001 cr and for year 2025 was N/A for company=JIOFIN\n\nAccumulated Depreciation for year 2016 was N/A and for year 2017 was N/A and for year 2018 was N/A and for year 2019 was N/A and for year 2020 was N/A and for year 2021 was N/A and for year 2022 was N/A and for year 2023 was -30.97 cr and for year 2024 was -40.04 cr and for year 2025 was N/A for company=JIOFIN\n\nAdditional Paid In Capital for year 2016 was 881.1700000000001 cr and for year 2017 was 881.1700000000001 cr and for year 2018 was 881.1700000000001 cr and for year 2019 was 881.1700000000001 cr and for year 2020 was 881.1700000000001 cr and for year 2021 was 881.1700000000001 cr and for year 2022 was 881.1700000000001 cr and for year 2023 was 29610.74 cr and for year 2024 was 29610.74 cr and for year 2025 was N/A for company=JIOFIN\n\nCash And Equivalents for year 2016 was 351.37 cr and for year 2017 was 352.36 cr and for year 2018 was 0.6799999999999999 cr and for year 2019 was 1.05 cr and for year 2020 was 0.49000000000000005 cr and for year 2021 was 0.36 cr and for year 2022 was 0.51 cr and for year 2023 was 63.120000000000005 cr and for year 2024 was 132.29000000000002 cr and for year 2025 was 4071.78 cr for company=JIOFIN\n\nCommon Stock, Total for year 2016 was 2.02 cr and for year 2017 was 2.33 cr and for year 2018 was 2.02 cr and for year 2019 was 2.02 cr and for year 2020 was 2.02 cr and for year 2021 was 2.02 cr and for year 2022 was 2.02 cr and for year 2023 was 2.02 cr and for year 2024 was 6353.280000000001 cr and for year 2025 was 6353.14 cr for company=JIOFIN\n\nComprehensive Income and Other for year 2016 was 1.54 cr and for year 2017 was 1.23 cr and for year 2018 was 1.23 cr and for year 2019 was 1.23 cr and for year 2020 was 1.23 cr and for year 2021 was 1.23 cr and for year 2022 was 1.23 cr and for year 2023 was 74024.83 cr and for year 2024 was 91097.51 cr and for year 2025 was 117143.38 cr for company=JIOFIN\n\nCurrent Income Taxes Payable for year 2016 was N/A and for year 2017 was 6.279999999999999 cr and for year 2018 was N/A and for year 2019 was N/A and for year 2020 was N/A and for year 2021 was N/A and for year 2022 was N/A and for year 2023 was N/A and for year 2024 was N/A and for year 2025 was N/A for company=JIOFIN\n\nDeferred Tax Assets Long-Term for year 2016 was N/A and for year 2017 was N/A and for year 2018 was N/A and for year 2019 was N/A and for year 2020 was N/A and for year 2021 was 3.94 cr and for year 2022 was 5.3100000000000005 cr and for year 2023 was 0.16999999999999998 cr and for year 2024 was 0.22999999999999998 cr and for year 2025 was 9.959999999999999 cr for company=JIOFIN\n\nDeferred Tax Liability Non Current for year 2016 was N/A and for year 2017 was N/A and for year 2018 was 20.97 cr and for year 2019 was 53.779999999999994 cr and for year 2020 was 4.7700000000000005 cr and for year 2021 was N/A and for year 2022 was N/A and for year 2023 was 6.8 cr and for year 2024 was 5557.61 cr and for year 2025 was 5565.01 cr for company=JIOFIN\n\nGoodwill for year 2016 was N/A and for year 2017 was N/A and for year 2018 was N/A and for year 2019 was N/A and for year 2020 was N/A and for year 2021 was N/A and for year 2022 was N/A and for year 2023 was 100.74 cr and for year 2024 was 100.47999999999999 cr and for year 2025 was 100.47999999999999 cr for company=JIOFIN\n\nGross Property Plant And Equipment for year 2016 was N/A and for year 2017 was N/A and for year 2018 was N/A and for year 2019 was N/A and for year 2020 was N/A and for year 2021 was N/A and for year 2022 was N/A and for year 2023 was 70.52000000000001 cr and for year 2024 was 71.31 cr and for year 2025 was N/A for company=JIOFIN\n\nLong-Term Debt for year 2016 was N/A and for year 2017 was N/A and for year 2018 was N/A and for year 2019 was N/A and for year 2020 was N/A and for year 2021 was N/A and for year 2022 was N/A and for year 2023 was N/A and for year 2024 was N/A and for year 2025 was 983.2299999999999 cr for company=JIOFIN\n\nLong-term Investments for year 2016 was 432.73999999999995 cr and for year 2017 was 255.8 cr and for year 2018 was N/A and for year 2019 was N/A and for year 2020 was 10.690000000000001 cr and for year 2021 was 10.690000000000001 cr and for year 2022 was 150.69 cr and for year 2023 was 97951.12 cr and for year 2024 was 127489.55 cr and for year 2025 was 118910.31999999999 cr for company=JIOFIN\n\nNet Property Plant And Equipment for year 2016 was N/A and for year 2017 was N/A and for year 2018 was N/A and for year 2019 was N/A and for year 2020 was N/A and for year 2021 was N/A and for year 2022 was N/A and for year 2023 was 39.55 cr and for year 2024 was 31.27 cr and for year 2025 was 39.260000000000005 cr for company=JIOFIN\n\nOther Current Assets, Total for year 2016 was 48.480000000000004 cr and for year 2017 was 0.01 cr and for year 2018 was 67.85 cr and for year 2019 was 880.1200000000001 cr and for year 2020 was 154.70999999999998 cr and for year 2021 was 47.35 cr and for year 2022 was 40.4 cr and for year 2023 was 6137.0599999999995 cr and for year 2024 was 10687.9 cr and for year 2025 was 168.06 cr for company=JIOFIN\n\nOther Intangibles, Total for year 2016 was N/A and for year 2017 was N/A and for year 2018 was N/A and for year 2019 was N/A and for year 2020 was N/A and for year 2021 was N/A and for year 2022 was N/A and for year 2023 was 55.8 cr and for year 2024 was 43.35 cr and for year 2025 was 54.65 cr for company=JIOFIN\n\nOther Long-Term Assets, Total for year 2016 was 0.01 cr and for year 2017 was N/A and for year 2018 was 0.01 cr and for year 2019 was N/A and for year 2020 was N/A and for year 2021 was N/A and for year 2022 was N/A and for year 2023 was N/A and for year 2024 was N/A and for year 2025 was N/A for company=JIOFIN\n\nOther Non Current Liabilities for year 2016 was N/A and for year 2017 was N/A and for year 2018 was 0.42000000000000004 cr and for year 2019 was 0.51 cr and for year 2020 was 7.8 cr and for year 2021 was 0.16999999999999998 cr and for year 2022 was 0.02 cr and for year 2023 was N/A and for year 2024 was N/A and for year 2025 was 445.11 cr for company=JIOFIN\n\nOther Receivables for year 2016 was 252.2 cr and for year 2017 was 70.56 cr and for year 2018 was 55.79 cr and for year 2019 was 67.56 cr and for year 2020 was 22.0 cr and for year 2021 was N/A and for year 2022 was N/A and for year 2023 was 351.27 cr and for year 2024 was 401.99 cr and for year 2025 was 102.31 cr for company=JIOFIN\n\nPension & Other Post Retirement Benefits for year 2016 was N/A and for year 2017 was N/A and for year 2018 was N/A and for year 2019 was N/A and for year 2020 was N/A and for year 2021 was N/A and for year 2022 was N/A and for year 2023 was 1.24 cr and for year 2024 was 3.3600000000000003 cr and for year 2025 was N/A for company=JIOFIN\n\nPreferred Stock Convertible for year 2016 was N/A and for year 2017 was N/A and for year 2018 was 0.31 cr and for year 2019 was 0.31 cr and for year 2020 was 0.31 cr and for year 2021 was 0.31 cr and for year 2022 was 0.31 cr and for year 2023 was 0.31 cr and for year 2024 was N/A and for year 2025 was N/A for company=JIOFIN\n\nRetained Earnings for year 2016 was 558.33 cr and for year 2017 was 761.55 cr and for year 2018 was 1223.94 cr and for year 2019 was 822.26 cr and for year 2020 was 1052.2 cr and for year 2021 was 1175.26 cr and for year 2022 was 1343.3 cr and for year 2023 was 10482.439999999999 cr and for year 2024 was 12086.130000000001 cr and for year 2025 was N/A for company=JIOFIN\n\nShort-term Borrowings for year 2016 was 3024.5 cr and for year 2017 was 1559.6 cr and for year 2018 was 1737.3 cr and for year 2019 was 2338.05 cr and for year 2020 was 2420.25 cr and for year 2021 was N/A and for year 2022 was N/A and for year 2023 was 742.77 cr and for year 2024 was N/A and for year 2025 was 2986.77 cr for company=JIOFIN\n\nTotal Assets for year 2016 was 4830.07 cr and for year 2017 was 3479.9 cr and for year 2018 was 3867.3599999999997 cr and for year 2019 was 4100.1900000000005 cr and for year 2020 was 4765.79 cr and for year 2021 was 2060.23 cr and for year 2022 was 2228.12 cr and for year 2023 was 114929.73999999999 cr and for year 2024 was 144862.99 cr and for year 2025 was 133509.94 cr for company=JIOFIN\n\nTotal Cash And Short Term Investments for year 2016 was 3854.46 cr and for year 2017 was 2988.07 cr and for year 2018 was 3712.1699999999996 cr and for year 2019 was 3144.8599999999997 cr and for year 2020 was 195.89000000000001 cr and for year 2021 was 47.160000000000004 cr and for year 2022 was 30.72 cr and for year 2023 was 10252.939999999999 cr and for year 2024 was 5934.91 cr and for year 2025 was 4071.78 cr for company=JIOFIN\n\nTotal Common Equity for year 2016 was 1443.06 cr and for year 2017 was 1646.28 cr and for year 2018 was 2108.3599999999997 cr and for year 2019 was 1706.6799999999998 cr and for year 2020 was 1936.6200000000001 cr and for year 2021 was 2059.68 cr and for year 2022 was 2227.7200000000003 cr and for year 2023 was 114120.03 cr and for year 2024 was 139147.66 cr and for year 2025 was 123496.51999999999 cr for company=JIOFIN\n\nTotal Current Assets for year 2016 was 4397.32 cr and for year 2017 was 3224.1 cr and for year 2018 was 3867.35 cr and for year 2019 was 4100.1900000000005 cr and for year 2020 was 4755.1 cr and for year 2021 was 2045.6 cr and for year 2022 was 2072.12 cr and for year 2023 was 16782.36 cr and for year 2024 was 17198.11 cr and for year 2025 was 14395.27 cr for company=JIOFIN\n\nTotal Current Liabilities for year 2016 was 3387.0099999999998 cr and for year 2017 was 1833.6200000000001 cr and for year 2018 was 1737.3 cr and for year 2019 was 2338.91 cr and for year 2020 was 2479.25 cr and for year 2021 was 0.06999999999999999 cr and for year 2022 was 0.06999999999999999 cr and for year 2023 was 801.36 cr and for year 2024 was 154.35999999999999 cr and for year 2025 was 3020.07 cr for company=JIOFIN\n\nTotal Equity for year 2016 was 1443.06 cr and for year 2017 was 1646.28 cr and for year 2018 was 2108.67 cr and for year 2019 was 1706.9900000000002 cr and for year 2020 was 1936.9299999999998 cr and for year 2021 was 2059.9900000000002 cr and for year 2022 was 2228.0299999999997 cr and for year 2023 was 114120.34 cr and for year 2024 was 139147.66 cr and for year 2025 was 123496.51999999999 cr for company=JIOFIN\n\nTotal Liabilities for year 2016 was 3387.0099999999998 cr and for year 2017 was 1833.6200000000001 cr and for year 2018 was 1758.69 cr and for year 2019 was 2393.2 cr and for year 2020 was 2828.8599999999997 cr and for year 2021 was 0.24 cr and for year 2022 was 0.09 cr and for year 2023 was 809.4 cr and for year 2024 was 5715.33 cr and for year 2025 was 10013.42 cr for company=JIOFIN\n\nTotal Liabilities And Equity for year 2016 was 4830.07 cr and for year 2017 was 3479.9 cr and for year 2018 was 3867.3599999999997 cr and for year 2019 was 4100.1900000000005 cr and for year 2020 was 4765.79 cr and for year 2021 was 2060.23 cr and for year 2022 was 2228.12 cr and for year 2023 was 114929.73999999999 cr and for year 2024 was 144862.99 cr and for year 2025 was 133509.94 cr for company=JIOFIN\n\nTotal Preferred Equity for year 2016 was N/A and for year 2017 was N/A and for year 2018 was 0.31 cr and for year 2019 was 0.31 cr and for year 2020 was 0.31 cr and for year 2021 was 0.31 cr and for year 2022 was 0.31 cr and for year 2023 was 0.31 cr and for year 2024 was N/A and for year 2025 was N/A for company=JIOFIN\n\nTotal Receivables for year 2016 was 494.38 cr and for year 2017 was 236.01999999999998 cr and for year 2018 was 87.33 cr and for year 2019 was 75.21000000000001 cr and for year 2020 was 4404.5 cr and for year 2021 was 1951.0900000000001 cr and for year 2022 was 2001.0 cr and for year 2023 was 392.36 cr and for year 2024 was 575.3 cr and for year 2025 was 10155.43 cr for company=JIOFIN\n\nTrading Asset Securities, Total for year 2016 was 3503.09 cr and for year 2017 was 2635.71 cr and for year 2018 was 3711.4900000000002 cr and for year 2019 was 3143.81 cr and for year 2020 was 195.4 cr and for year 2021 was 46.8 cr and for year 2022 was 30.21 cr and for year 2023 was 10189.82 cr and for year 2024 was 5802.62 cr and for year 2025 was N/A for company=JIOFIN\n\nUnearned Revenue Non Current for year 2016 was N/A and for year 2017 was N/A and for year 2018 was N/A and for year 2019 was N/A and for year 2020 was 337.04 cr and for year 2021 was N/A and for year 2022 was N/A and for year 2023 was N/A and for year 2024 was N/A and for year 2025 was N/A for company=JIOFIN\n\nOther Current Liabilities for year 2016 was 29.85 cr and for year 2017 was 27.830000000000002 cr and for year 2018 was N/A and for year 2019 was N/A and for year 2020 was N/A and for year 2021 was N/A and for year 2022 was N/A and for year 2023 was 0.11000000000000001 cr and for year 2024 was 6.95 cr and for year 2025 was N/A for company=JIOFIN\n\n--- Cashflow Data ---\n\n--- Data for TCS ---\nCapital Expenditure for year 2016 was -1987.0 cr and for year 2017 was -1989.0 cr and for year 2018 was -1862.0 cr and for year 2019 was -2053.0 cr and for year 2020 was -2538.0 cr and for year 2021 was -2719.0 cr and for year 2022 was -2483.0 cr and for year 2023 was -2532.0 cr and for year 2024 was -2202.0 cr and for year 2025 was -3980.0 cr for company=TCS\n\nNet Income for year 2016 was 24270.0 cr and for year 2017 was 26289.0 cr and for year 2018 was 25826.0 cr and for year 2019 was 31472.0 cr and for year 2020 was 32340.0 cr and for year 2021 was 32430.0 cr and for year 2022 was 38327.0 cr and for year 2023 was 42147.0 cr and for year 2024 was 45908.0 cr and for year 2025 was 48553.0 cr for company=TCS\n\nDepreciation & Amortization for year 2016 was 1791.0 cr and for year 2017 was 1906.0 cr and for year 2018 was 1977.0 cr and for year 2019 was 2017.0 cr and for year 2020 was 3440.0 cr and for year 2021 was 4001.0 cr and for year 2022 was 4249.0 cr and for year 2023 was 4526.0 cr and for year 2024 was 4506.0 cr and for year 2025 was 4515.0 cr for company=TCS\n\nAmortization of Goodwill and Intangible Assets for year 2016 was 97.0 cr and for year 2017 was 81.0 cr and for year 2018 was 37.0 cr and for year 2019 was 39.0 cr and for year 2020 was 89.0 cr and for year 2021 was 64.0 cr and for year 2022 was 355.0 cr and for year 2023 was 496.0 cr and for year 2024 was 479.0 cr and for year 2025 was 727.0 cr for company=TCS\n\nDepreciation & Amortization, Total for year 2016 was 1888.0 cr and for year 2017 was 1987.0 cr and for year 2018 was 2014.0 cr and for year 2019 was 2056.0 cr and for year 2020 was 3529.0 cr and for year 2021 was 4065.0 cr and for year 2022 was 4604.0 cr and for year 2023 was 5022.0 cr and for year 2024 was 4985.0 cr and for year 2025 was 5242.0 cr for company=TCS\n\nProvision and Write-off of Bad Debts for year 2016 was 135.0 cr and for year 2017 was 125.0 cr and for year 2018 was 206.0 cr and for year 2019 was 187.0 cr and for year 2020 was 144.0 cr and for year 2021 was 201.0 cr and for year 2022 was 135.0 cr and for year 2023 was 140.0 cr and for year 2024 was 114.0 cr and for year 2025 was 128.0 cr for company=TCS\n\nOther Operating Activities, Total for year 2016 was -1770.0 cr and for year 2017 was -1902.0 cr and for year 2018 was -1839.0 cr and for year 2019 was -2442.0 cr and for year 2020 was 1283.0 cr and for year 2021 was 1460.0 cr and for year 2022 was -127.0 cr and for year 2023 was -877.0 cr and for year 2024 was 532.0 cr and for year 2025 was -1367.0 cr for company=TCS\n\nChange In Accounts Receivable for year 2016 was -2987.0 cr and for year 2017 was -859.0 cr and for year 2018 was -3274.0 cr and for year 2019 was -1597.0 cr and for year 2020 was -3803.0 cr and for year 2021 was 1059.0 cr and for year 2022 was -5144.0 cr and for year 2023 was -7683.0 cr and for year 2024 was -3332.0 cr and for year 2025 was -5228.0 cr for company=TCS\n\nChange In Inventories for year 2016 was N/A and for year 2017 was N/A and for year 2018 was -5.0 cr and for year 2019 was 16.0 cr and for year 2020 was 5.0 cr and for year 2021 was -3.0 cr and for year 2022 was -12.0 cr and for year 2023 was -8.0 cr and for year 2024 was N/A and for year 2025 was 7.0 cr for company=TCS\n\nChange In Accounts Payable for year 2016 was -2039.0 cr and for year 2017 was -201.0 cr and for year 2018 was -346.0 cr and for year 2019 was 1496.0 cr and for year 2020 was 446.0 cr and for year 2021 was -93.0 cr and for year 2022 was 186.0 cr and for year 2023 was 2036.0 cr and for year 2024 was -632.0 cr and for year 2025 was 3718.0 cr for company=TCS\n\nChange in Unearned Revenues for year 2016 was 262.0 cr and for year 2017 was 80.0 cr and for year 2018 was 1104.0 cr and for year 2019 was 679.0 cr and for year 2020 was 375.0 cr and for year 2021 was 1091.0 cr and for year 2022 was -103.0 cr and for year 2023 was 39.0 cr and for year 2024 was -740.0 cr and for year 2025 was 395.0 cr for company=TCS\n\nChange in Other Net Operating Assets for year 2016 was -180.0 cr and for year 2017 was 349.0 cr and for year 2018 was 2312.0 cr and for year 2019 was -2763.0 cr and for year 2020 was -1690.0 cr and for year 2021 was -1191.0 cr and for year 2022 was 2304.0 cr and for year 2023 was 1399.0 cr and for year 2024 was -2178.0 cr and for year 2025 was -2251.0 cr for company=TCS\n\nCash from Operations for year 2016 was 19109.0 cr and for year 2017 was 25223.0 cr and for year 2018 was 25067.0 cr and for year 2019 was 28593.0 cr and for year 2020 was 32369.0 cr and for year 2021 was 38802.0 cr and for year 2022 was 39949.0 cr and for year 2023 was 41965.0 cr and for year 2024 was 44338.0 cr and for year 2025 was 48908.0 cr for company=TCS\n\n(Gain) Loss From Sale Of Asset for year 2016 was -5.0 cr and for year 2017 was -3.0 cr and for year 2018 was -25.0 cr and for year 2019 was -84.0 cr and for year 2020 was -46.0 cr and for year 2021 was -13.0 cr and for year 2022 was -23.0 cr and for year 2023 was -26.0 cr and for year 2024 was -7.0 cr and for year 2025 was -20.0 cr for company=TCS\n\n(Gain) Loss on Sale of Investments for year 2016 was -465.0 cr and for year 2017 was -642.0 cr and for year 2018 was -906.0 cr and for year 2019 was -427.0 cr and for year 2020 was -214.0 cr and for year 2021 was -204.0 cr and for year 2022 was -198.0 cr and for year 2023 was -224.0 cr and for year 2024 was -312.0 cr and for year 2025 was -269.0 cr for company=TCS\n\nSale of Property, Plant, and Equipment for year 2016 was 22.0 cr and for year 2017 was 36.0 cr and for year 2018 was 58.0 cr and for year 2019 was 99.0 cr and for year 2020 was 161.0 cr and for year 2021 was 37.0 cr and for year 2022 was 31.0 cr and for year 2023 was 37.0 cr and for year 2024 was 17.0 cr and for year 2025 was 23.0 cr for company=TCS\n\nCash Acquisitions for year 2016 was N/A and for year 2017 was N/A and for year 2018 was N/A and for year 2019 was -50.0 cr and for year 2020 was N/A and for year 2021 was N/A and for year 2022 was N/A and for year 2023 was N/A and for year 2024 was N/A and for year 2025 was N/A for company=TCS\n\nSale (Purchase) of Intangible assets for year 2016 was -3.0 cr and for year 2017 was N/A and for year 2018 was N/A and for year 2019 was -178.0 cr and for year 2020 was -192.0 cr and for year 2021 was -356.0 cr and for year 2022 was -497.0 cr and for year 2023 was -355.0 cr and for year 2024 was -435.0 cr and for year 2025 was -944.0 cr for company=TCS\n\nInvestment in Marketable and Equity Securities, Total for year 2016 was -3394.0 cr and for year 2017 was -18587.0 cr and for year 2018 was 4383.0 cr and for year 2019 was 4068.0 cr and for year 2020 was 8389.0 cr and for year 2021 was -4670.0 cr and for year 2022 was -5519.0 cr and for year 2023 was -5354.0 cr and for year 2024 was 4811.0 cr and for year 2025 was -586.0 cr for company=TCS\n\nNet (Increase) Decrease in Loans Originated / Sold - Investing for year 2016 was N/A and for year 2017 was N/A and for year 2018 was N/A and for year 2019 was N/A and for year 2020 was N/A and for year 2021 was N/A and for year 2022 was N/A and for year 2023 was N/A and for year 2024 was N/A and for year 2025 was -26.0 cr for company=TCS\n\nOther Investing Activities, Total for year 2016 was 212.0 cr and for year 2017 was 3808.0 cr and for year 2018 was 307.0 cr and for year 2019 was -290.0 cr and for year 2020 was 2745.0 cr and for year 2021 was -421.0 cr and for year 2022 was 7571.0 cr and for year 2023 was 8243.0 cr and for year 2024 was 3835.0 cr and for year 2025 was 3195.0 cr for company=TCS\n\nCash from Investing for year 2016 was -5150.0 cr and for year 2017 was -16732.0 cr and for year 2018 was 2886.0 cr and for year 2019 was 1596.0 cr and for year 2020 was 8565.0 cr and for year 2021 was -8129.0 cr and for year 2022 was -897.0 cr and for year 2023 was 39.0 cr and for year 2024 was 6026.0 cr and for year 2025 was -2318.0 cr for company=TCS\n\nShort Term Debt Issued, Total for year 2016 was N/A and for year 2017 was 87.0 cr and for year 2018 was N/A and for year 2019 was N/A and for year 2020 was N/A and for year 2021 was N/A and for year 2022 was N/A and for year 2023 was N/A and for year 2024 was N/A and for year 2025 was N/A for company=TCS\n\nTotal Debt Issued for year 2016 was N/A and for year 2017 was 87.0 cr and for year 2018 was N/A and for year 2019 was N/A and for year 2020 was N/A and for year 2021 was N/A and for year 2022 was N/A and for year 2023 was N/A and for year 2024 was N/A and for year 2025 was N/A for company=TCS\n\nShort Term Debt Repaid, Total for year 2016 was -73.0 cr and for year 2017 was N/A and for year 2018 was -19.0 cr and for year 2019 was -181.0 cr and for year 2020 was N/A and for year 2021 was N/A and for year 2022 was N/A and for year 2023 was N/A and for year 2024 was N/A and for year 2025 was N/A for company=TCS\n\nLong-Term Debt Repaid, Total for year 2016 was -60.0 cr and for year 2017 was -66.0 cr and for year 2018 was -24.0 cr and for year 2019 was -13.0 cr and for year 2020 was -1062.0 cr and for year 2021 was -1336.0 cr and for year 2022 was -1417.0 cr and for year 2023 was -1515.0 cr and for year 2024 was -1614.0 cr and for year 2025 was -1664.0 cr for company=TCS\n\nTotal Debt Repaid for year 2016 was -133.0 cr and for year 2017 was -66.0 cr and for year 2018 was -43.0 cr and for year 2019 was -194.0 cr and for year 2020 was -1062.0 cr and for year 2021 was -1336.0 cr and for year 2022 was -1417.0 cr and for year 2023 was -1515.0 cr and for year 2024 was -1614.0 cr and for year 2025 was -1664.0 cr for company=TCS\n\nRepurchase of Common Stock for year 2016 was N/A and for year 2017 was N/A and for year 2018 was -16000.0 cr and for year 2019 was -16000.0 cr and for year 2020 was N/A and for year 2021 was -19726.0 cr and for year 2022 was -18000.0 cr and for year 2023 was -4192.0 cr and for year 2024 was -20959.0 cr and for year 2025 was N/A for company=TCS\n\nCommon Dividends Paid for year 2016 was -7993.0 cr and for year 2017 was -9162.0 cr and for year 2018 was -9284.0 cr and for year 2019 was -10085.0 cr and for year 2020 was -31896.0 cr and for year 2021 was -10850.0 cr and for year 2022 was -13317.0 cr and for year 2023 was -41347.0 cr and for year 2024 was -25137.0 cr and for year 2025 was -44864.0 cr for company=TCS\n\nCommon & Preferred Stock Dividends Paid for year 2016 was -7993.0 cr and for year 2017 was -9162.0 cr and for year 2018 was -9284.0 cr and for year 2019 was -10085.0 cr and for year 2020 was -31896.0 cr and for year 2021 was -10850.0 cr and for year 2022 was -13317.0 cr and for year 2023 was -41347.0 cr and for year 2024 was -25137.0 cr and for year 2025 was -44864.0 cr for company=TCS\n\nOther Financing Activities, Total for year 2016 was -1540.0 cr and for year 2017 was -1885.0 cr and for year 2018 was -1558.0 cr and for year 2019 was -1618.0 cr and for year 2020 was -6957.0 cr and for year 2021 was -722.0 cr and for year 2022 was -847.0 cr and for year 2023 was -824.0 cr and for year 2024 was -826.0 cr and for year 2025 was -910.0 cr for company=TCS\n\nCash from Financing for year 2016 was -9666.0 cr and for year 2017 was -11026.0 cr and for year 2018 was -26885.0 cr and for year 2019 was -27897.0 cr and for year 2020 was -39915.0 cr and for year 2021 was -32634.0 cr and for year 2022 was -33581.0 cr and for year 2023 was -47878.0 cr and for year 2024 was -48536.0 cr and for year 2025 was -47438.0 cr for company=TCS\n\nNet Change in Cash for year 2016 was 4433.0 cr and for year 2017 was -2698.0 cr and for year 2018 was 1286.0 cr and for year 2019 was 2341.0 cr and for year 2020 was 1422.0 cr and for year 2021 was -1788.0 cr and for year 2022 was 5630.0 cr and for year 2023 was -5365.0 cr and for year 2024 was 1893.0 cr and for year 2025 was -674.0 cr for company=TCS\n\nNOPAT for year 2016 was 22024.989999999998 cr and for year 2017 was 23170.94 cr and for year 2018 was 23166.920000000002 cr and for year 2019 was 28443.26 cr and for year 2020 was 29630.71 cr and for year 2021 was 31611.07 cr and for year 2022 was 36036.62 cr and for year 2023 was 40318.21 cr and for year 2024 was 44101.82 cr and for year 2025 was 46434.520000000004 cr for company=TCS\n\nForeign Exchange Rate Adjustments for year 2016 was 140.0 cr and for year 2017 was -163.0 cr and for year 2018 was 218.0 cr and for year 2019 was 49.0 cr and for year 2020 was 403.0 cr and for year 2021 was 173.0 cr and for year 2022 was 159.0 cr and for year 2023 was 509.0 cr and for year 2024 was 65.0 cr and for year 2025 was 174.0 cr for company=TCS\n\n--- Data for JIOFIN ---\nProvision for Credit Losses for year 2016 was N/A and for year 2017 was N/A and for year 2018 was -0.95 cr and for year 2019 was -0.09 cr and for year 2020 was 17.61 cr and for year 2021 was -7.840000000000001 cr and for year 2022 was 0.25 cr and for year 2023 was N/A and for year 2024 was N/A and for year 2025 was N/A for company=JIOFIN\n\nOther Operating Activities, Total for year 2016 was 174.07999999999998 cr and for year 2017 was 169.81 cr and for year 2018 was -181.45999999999998 cr and for year 2019 was 140.66 cr and for year 2020 was -251.73000000000002 cr and for year 2021 was -99.42 cr and for year 2022 was -154.99 cr and for year 2023 was 25.6 cr and for year 2024 was -1125.4 cr and for year 2025 was -880.6299999999999 cr for company=JIOFIN\n\nChange In Accounts Receivable for year 2016 was N/A and for year 2017 was N/A and for year 2018 was N/A and for year 2019 was 39.64 cr and for year 2020 was 307.8 cr and for year 2021 was 308.65 cr and for year 2022 was 148.09 cr and for year 2023 was N/A and for year 2024 was N/A and for year 2025 was N/A for company=JIOFIN\n\nChange In Inventories for year 2016 was -439.86 cr and for year 2017 was 898.6899999999999 cr and for year 2018 was N/A and for year 2019 was N/A and for year 2020 was N/A and for year 2021 was N/A and for year 2022 was N/A and for year 2023 was N/A and for year 2024 was N/A and for year 2025 was N/A for company=JIOFIN\n\nChange In Accounts Payable for year 2016 was 73.47 cr and for year 2017 was -73.96000000000001 cr and for year 2018 was 0.05 cr and for year 2019 was 0.82 cr and for year 2020 was 336.2 cr and for year 2021 was -336.95 cr and for year 2022 was -0.15 cr and for year 2023 was 0.04 cr and for year 2024 was 96.14 cr and for year 2025 was 291.05 cr for company=JIOFIN\n\nChange in Other Net Operating Assets for year 2016 was -438.76000000000005 cr and for year 2017 was 53.89 cr and for year 2018 was 403.73 cr and for year 2019 was 20.080000000000002 cr and for year 2020 was -4341.95 cr and for year 2021 was 2434.38 cr and for year 2022 was -37.68 cr and for year 2023 was 2011.0900000000001 cr and for year 2024 was -300.28000000000003 cr and for year 2025 was -9982.18 cr for company=JIOFIN\n\nCash from Operations for year 2016 was -919.61 cr and for year 2017 was 1378.4 cr and for year 2018 was 173.2 cr and for year 2019 was -30.65 cr and for year 2020 was -3686.75 cr and for year 2021 was 2434.13 cr and for year 2022 was 123.04 cr and for year 2023 was 2054.9 cr and for year 2024 was -677.5699999999999 cr and for year 2025 was -10083.39 cr for company=JIOFIN\n\n(Gain) Loss on Sale of Investments for year 2016 was 12.290000000000001 cr and for year 2017 was N/A and for year 2018 was -289.86 cr and for year 2019 was 169.92000000000002 cr and for year 2020 was 15.38 cr and for year 2021 was 12.25 cr and for year 2022 was -0.52 cr and for year 2023 was -10.059999999999999 cr and for year 2024 was 2.05 cr and for year 2025 was 40.35 cr for company=JIOFIN\n\n(Income) Loss On Equity Investments for year 2016 was N/A and for year 2017 was N/A and for year 2018 was N/A and for year 2019 was N/A and for year 2020 was N/A and for year 2021 was N/A and for year 2022 was N/A and for year 2023 was N/A and for year 2024 was -428.52 cr and for year 2025 was -392.82 cr for company=JIOFIN\n\nCapital Expenditure for year 2016 was N/A and for year 2017 was N/A and for year 2018 was N/A and for year 2019 was N/A and for year 2020 was N/A and for year 2021 was N/A and for year 2022 was N/A and for year 2023 was N/A and for year 2024 was -0.79 cr and for year 2025 was -41.8 cr for company=JIOFIN\n\nDivestitures for year 2016 was N/A and for year 2017 was N/A and for year 2018 was N/A and for year 2019 was N/A and for year 2020 was N/A and for year 2021 was N/A and for year 2022 was N/A and for year 2023 was N/A and for year 2024 was 0.3 cr and for year 2025 was N/A for company=JIOFIN\n\nInvestment in Marketable and Equity Securities, Total for year 2016 was 27.2 cr and for year 2017 was 192.85 cr and for year 2018 was -216.03000000000003 cr and for year 2019 was -352.23 cr and for year 2020 was 3647.35 cr and for year 2021 was 163.22 cr and for year 2022 was -122.89000000000001 cr and for year 2023 was -1166.59 cr and for year 2024 was 265.08000000000004 cr and for year 2025 was 5184.99 cr for company=JIOFIN\n\nNet (Increase) Decrease in Loans Originated / Sold - Investing for year 2016 was N/A and for year 2017 was 76.72 cr and for year 2018 was 133.92000000000002 cr and for year 2019 was N/A and for year 2020 was N/A and for year 2021 was N/A and for year 2022 was N/A and for year 2023 was N/A and for year 2024 was N/A and for year 2025 was N/A for company=JIOFIN\n\nOther Investing Activities, Total for year 2016 was 145.95 cr and for year 2017 was 105.30999999999999 cr and for year 2018 was 68.39 cr and for year 2019 was 46.8 cr and for year 2020 was 1.48 cr and for year 2021 was N/A and for year 2022 was N/A and for year 2023 was 0.27 cr and for year 2024 was 1176.6299999999999 cr and for year 2025 was 1262.99 cr for company=JIOFIN\n\nCash from Investing for year 2016 was 173.15 cr and for year 2017 was 374.88 cr and for year 2018 was -13.719999999999999 cr and for year 2019 was -305.43 cr and for year 2020 was 3648.8300000000004 cr and for year 2021 was 163.22 cr and for year 2022 was -122.89000000000001 cr and for year 2023 was -1166.3200000000002 cr and for year 2024 was 1441.22 cr and for year 2025 was 6406.18 cr for company=JIOFIN\n\nShort Term Debt Issued, Total for year 2016 was 5469.9400000000005 cr and for year 2017 was 5133.719999999999 cr and for year 2018 was 4122.45 cr and for year 2019 was 584.12 cr and for year 2020 was 98.83 cr and for year 2021 was N/A and for year 2022 was N/A and for year 2023 was N/A and for year 2024 was 3967.69 cr and for year 2025 was N/A for company=JIOFIN\n\nTotal Debt Issued for year 2016 was 5469.9400000000005 cr and for year 2017 was 5133.719999999999 cr and for year 2018 was 4122.45 cr and for year 2019 was 584.12 cr and for year 2020 was 98.83 cr and for year 2021 was N/A and for year 2022 was N/A and for year 2023 was N/A and for year 2024 was 3967.69 cr and for year 2025 was N/A for company=JIOFIN\n\nShort Term Debt Repaid, Total for year 2016 was -4566.83 cr and for year 2017 was -6598.62 cr and for year 2018 was -3944.75 cr and for year 2019 was N/A and for year 2020 was N/A and for year 2021 was -2420.25 cr and for year 2022 was N/A and for year 2023 was -742.77 cr and for year 2024 was N/A and for year 2025 was N/A for company=JIOFIN\n\nTotal Debt Repaid for year 2016 was -4566.83 cr and for year 2017 was -6598.62 cr and for year 2018 was -3944.75 cr and for year 2019 was N/A and for year 2020 was N/A and for year 2021 was -2420.25 cr and for year 2022 was N/A and for year 2023 was -742.77 cr and for year 2024 was N/A and for year 2025 was N/A for company=JIOFIN\n\nCommon & Preferred Stock Dividends Paid for year 2016 was N/A and for year 2017 was N/A and for year 2018 was N/A and for year 2019 was N/A and for year 2020 was N/A and for year 2021 was N/A and for year 2022 was -888.9200000000001 cr and for year 2023 was N/A and for year 2024 was N/A and for year 2025 was N/A for company=JIOFIN\n\nOther Financing Activities, Total for year 2016 was -156.66 cr and for year 2017 was -287.39 cr and for year 2018 was -337.86 cr and for year 2019 was -247.67 cr and for year 2020 was -61.470000000000006 cr and for year 2021 was -177.23 cr and for year 2022 was N/A and for year 2023 was -10.27 cr and for year 2024 was -5.34 cr and for year 2025 was N/A for company=JIOFIN\n\nCash from Financing for year 2016 was 746.45 cr and for year 2017 was -1752.2900000000002 cr and for year 2018 was -160.16 cr and for year 2019 was 336.45 cr and for year 2020 was 37.36 cr and for year 2021 was -2597.48 cr and for year 2022 was -888.9200000000001 cr and for year 2023 was -753.04 cr and for year 2024 was 3962.35 cr and for year 2025 was N/A for company=JIOFIN\n\nNet Change in Cash for year 2016 was -0.01 cr and for year 2017 was 0.99 cr and for year 2018 was -0.6799999999999999 cr and for year 2019 was 0.37 cr and for year 2020 was -0.5599999999999999 cr and for year 2021 was -0.13 cr and for year 2022 was 0.15 cr and for year 2023 was 56.06 cr and for year 2024 was 10.61 cr and for year 2025 was 285.14 cr for company=JIOFIN\n\nNOPAT for year 2016 was -300.16999999999996 cr and for year 2017 was 330.54 cr and for year 2018 was 245.36999999999998 cr and for year 2019 was -396.24 cr and for year 2020 was 229.95 cr and for year 2021 was 127.85 cr and for year 2022 was 168.14000000000001 cr and for year 2023 was 29.22 cr and for year 2024 was 1252.35 cr and for year 2025 was 1257.4 cr for company=JIOFIN\n\nMiscellaneous Cash Flow Adjustments for year 2016 was 56.4 cr and for year 2017 was N/A and for year 2018 was N/A and for year 2019 was N/A and for year 2020 was N/A and for year 2021 was N/A and for year 2022 was N/A and for year 2023 was N/A and for year 2024 was N/A and for year 2025 was N/A for company=JIOFIN\n\nNet Income for year 2016 was -300.83000000000004 cr and for year 2017 was 329.96999999999997 cr and for year 2018 was 241.69 cr and for year 2019 was -401.68 cr and for year 2020 was 229.94 cr and for year 2021 was 123.05999999999999 cr and for year 2022 was 168.04000000000002 cr and for year 2023 was 31.25 cr and for year 2024 was 1604.55 cr and for year 2025 was 1612.59 cr for company=JIOFIN\n\nDepreciation & Amortization for year 2016 was N/A and for year 2017 was N/A and for year 2018 was N/A and for year 2019 was N/A and for year 2020 was N/A and for year 2021 was N/A and for year 2022 was N/A and for year 2023 was N/A and for year 2024 was 9.07 cr and for year 2025 was 22.52 cr for company=JIOFIN\n\nDepreciation & Amortization, Total for year 2016 was N/A and for year 2017 was N/A and for year 2018 was N/A and for year 2019 was N/A and for year 2020 was N/A and for year 2021 was N/A and for year 2022 was N/A and for year 2023 was N/A and for year 2024 was 9.07 cr and for year 2025 was 22.52 cr for company=JIOFIN\n\nAmortization of Deferred Charges, Total for year 2016 was N/A and for year 2017 was N/A and for year 2018 was N/A and for year 2019 was N/A and for year 2020 was N/A and for year 2021 was N/A and for year 2022 was N/A and for year 2023 was N/A and for year 2024 was 12.45 cr and for year 2025 was N/A for company=JIOFIN\n\nAsset Writedown & Restructuring Costs for year 2016 was N/A and for year 2017 was N/A and for year 2018 was N/A and for year 2019 was N/A and for year 2020 was N/A and for year 2021 was N/A and for year 2022 was N/A and for year 2023 was -3.02 cr and for year 2024 was -547.63 cr and for year 2025 was -794.27 cr for company=JIOFIN\n\n--- Financial Ratios Data ---\n\n--- Data for TCS ---\ndebt_to_assets for year 2016 was 0.198314% and for year 2017 was 0.161469% and for year 2018 was 0.195360% and for year 2019 was 0.217882% and for year 2020 was 0.299010% and for year 2021 was 0.333828% and for year 2022 was 0.365109% and for year 2023 was 0.365086% and for year 2024 was 0.376445% and for year 2025 was 0.400040% for company=TCS.\n\ndepreciation_to_net_profit for year 2016 was 0.077792% and for year 2017 was 0.075583% and for year 2018 was 0.077983% and for year 2019 was 0.065328% and for year 2020 was 0.109122% and for year 2021 was 0.125347% and for year 2022 was 0.120124% and for year 2023 was 0.119154% and for year 2024 was 0.108587% and for year 2025 was 0.107964% for company=TCS.\n\nreceivables_turnover for year 2016 was 3.871228 and for year 2017 was 4.239569 and for year 2018 was 3.892124 and for year 2019 was 4.097899 and for year 2020 was 3.869933 and for year 2021 was 4.054554 and for year 2022 was 4.163316 and for year 2023 was 4.057189 and for year 2024 was 4.053868 and for year 2025 was 3.911273 for company=TCS.\n\ncurrent_ratio for year 2016 was 4.060184 and for year 2017 was 5.534454 and for year 2018 was 4.555979 and for year 2019 was 4.171844 and for year 2020 was 3.334701 and for year 2021 was 2.906749 and for year 2022 was 2.557437 and for year 2023 was 2.531567 and for year 2024 was 2.450633 and for year 2025 was 2.320918 for company=TCS.\n\nsustainable_growth_rate for year 2016 was 0.451692% and for year 2017 was 0.409459% and for year 2018 was 0.410499% and for year 2019 was 0.462263% and for year 2020 was 0.757956% and for year 2021 was 0.496854% and for year 2022 was 0.574806% and for year 2023 was 0.915444% and for year 2024 was 0.777987% and for year 2025 was 0.975421% for company=TCS.\n\ninterest_coverage for year 2016 was -873.151515 and for year 2017 was -948.156250 and for year 2018 was -586.884615 and for year 2019 was -189.171717 and for year 2020 was -41.754329 and for year 2021 was -66.690738 and for year 2022 was -61.790816 and for year 2023 was -69.623877 and for year 2024 was -76.235219 and for year 2025 was -78.100503 for company=TCS.\n\ncapex_to_operating_cash_flow for year 2016 was -0.103982% and for year 2017 was -0.078857% and for year 2018 was -0.074281% and for year 2019 was -0.071801% and for year 2020 was -0.078408% and for year 2021 was -0.070074% and for year 2022 was -0.062154% and for year 2023 was -0.060336% and for year 2024 was -0.049664% and for year 2025 was -0.081377% for company=TCS.\n\noperating_cash_flow_ratio for year 2016 was 1.227375 and for year 2017 was 1.738079 and for year 2018 was 1.406047 and for year 2019 was 1.294738 and for year 2020 was 1.196194 and for year 2021 was 1.136056 and for year 2022 was 0.943284 and for year 2023 was 0.963428 and for year 2024 was 0.961695 and for year 2025 was 0.922775 for company=TCS.\n\nnet_working_capital for year 2016 was 476440.000000 and for year 2017 was 658040.000000 and for year 2018 was 633960.000000 and for year 2019 was 700470.000000 and for year 2020 was 631770.000000 and for year 2021 was 651250.000000 and for year 2022 was 659590.000000 and for year 2023 was 667120.000000 and for year 2024 was 668800.000000 and for year 2025 was 700100.000000 for company=TCS.\n\nprice_to_earnings_ratio for year 2016 was 19.922898 and for year 2017 was 18.227168 and for year 2018 was 21.118822 and for year 2019 was 23.865631 and for year 2020 was 19.193630 and for year 2021 was 36.101300 and for year 2022 was 35.884995 and for year 2023 was 27.832844 and for year 2024 was 30.549885 and for year 2025 was 26.872513 for company=TCS.\n\nasset_turnover for year 2016 was 1.219426 and for year 2017 was 1.142506 and for year 2018 was 1.158124 and for year 2019 was 1.274223 and for year 2020 was 1.298183 and for year 2021 was 1.255569 and for year 2022 was 1.355018 and for year 2023 was 1.569484 and for year 2024 was 1.644893 and for year 2025 was 1.599484 for company=TCS.\n\ncash_earnings_per_share for year 2016 was 66.375701 and for year 2017 was 71.750108 and for year 2018 was 72.715875 and for year 2019 was 89.350815 and for year 2020 was 95.589489 and for year 2021 was 98.659133 and for year 2022 was 117.326665 and for year 2023 was 128.908748 and for year 2024 was 140.662226 and for year 2025 was 148.683010 for company=TCS.\n\nquick_ratio for year 2016 was 4.059156 and for year 2017 was 5.533007 and for year 2018 was 4.554521 and for year 2019 was 4.171391 and for year 2020 was 3.334516 and for year 2021 was 2.906514 and for year 2022 was 2.556964 and for year 2023 was 2.530924 and for year 2024 was 2.450026 and for year 2025 was 2.320522 for company=TCS.\n\ncash_return_on_assets for year 2016 was 0.214477% and for year 2017 was 0.244286% and for year 2018 was 0.235823% and for year 2019 was 0.248758% and for year 2020 was 0.267736% and for year 2021 was 0.296744% and for year 2022 was 0.282297% and for year 2023 was 0.292132% and for year 2024 was 0.302754% and for year 2025 was 0.306385% for company=TCS.\n\ndividend_per_share for year 2016 was -20.282169 and for year 2017 was -23.248497 and for year 2018 was -24.249073 and for year 2019 was -26.876133 and for year 2020 was -85.001599 and for year 2021 was -29.331459 and for year 2022 was -36.394195 and for year 2023 was -112.997732 and for year 2024 was -69.475692 and for year 2025 was -123.998784 for company=TCS.\n\nequity_ratio for year 2016 was 0.801686% and for year 2017 was 0.838531% and for year 2018 was 0.804640% and for year 2019 was 0.782118% and for year 2020 was 0.700990% and for year 2021 was 0.666172% and for year 2022 was 0.634891% and for year 2023 was 0.634914% and for year 2024 was 0.623555% and for year 2025 was 0.599960% for company=TCS.\n\ndividend_coverage_ratio for year 2016 was -2.390717 and for year 2017 was -2.753002 and for year 2018 was -2.700022 and for year 2019 was -2.835201 and for year 2020 was -1.014829 and for year 2021 was -3.576221 and for year 2022 was -2.999850 and for year 2023 was -1.014947 and for year 2024 was -1.763854 and for year 2025 was -1.090139 for company=TCS.\n\ndepreciation_to_sales for year 2016 was 0.017378% and for year 2017 was 0.016844% and for year 2018 was 0.016360% and for year 2019 was 0.014038% and for year 2020 was 0.022485% and for year 2021 was 0.024760% and for year 2022 was 0.024010% and for year 2023 was 0.022275% and for year 2024 was 0.020694% and for year 2025 was 0.020531% for company=TCS.\n\nnet_income_to_operating_cf_ratio for year 2016 was 1.270082 and for year 2017 was 1.042263 and for year 2018 was 1.030279 and for year 2019 was 1.100689 and for year 2020 was 0.999104 and for year 2021 was 0.835782 and for year 2022 was 0.959398 and for year 2023 was 1.004337 and for year 2024 was 1.035410 and for year 2025 was 0.992741 for company=TCS.\n\neffective_interest_burden for year 2016 was -0.001052% and for year 2017 was -0.000945% and for year 2018 was -0.001568% and for year 2019 was -0.004823% and for year 2020 was -0.022013% and for year 2021 was -0.014263% and for year 2022 was -0.015235% and for year 2023 was -0.013749% and for year 2024 was -0.012422% and for year 2025 was -0.012241% for company=TCS.\n\nworking_capital_to_total_assets for year 2016 was 0.534749% and for year 2017 was 0.637315% and for year 2018 was 0.596410% and for year 2019 was 0.609406% and for year 2020 was 0.522560% and for year 2021 was 0.498054% and for year 2022 was 0.466095% and for year 2023 was 0.464403% and for year 2024 was 0.456678% and for year 2025 was 0.438579% for company=TCS.\n\nfinancial_leverage for year 2016 was 1.247371 and for year 2017 was 1.192562 and for year 2018 was 1.242792 and for year 2019 was 1.278579 and for year 2020 was 1.426554 and for year 2021 was 1.501114 and for year 2022 was 1.575073 and for year 2023 was 1.575017 and for year 2024 was 1.603708 and for year 2025 was 1.666778 for company=TCS.\n\nretained_earnings_to_equity for year 2016 was 0.451692% and for year 2017 was 0.409459% and for year 2018 was 0.410499% and for year 2019 was 0.462263% and for year 2020 was 0.757956% and for year 2021 was 0.496854% and for year 2022 was 0.574806% and for year 2023 was 0.915444% and for year 2024 was 0.777987% and for year 2025 was 0.975421% for company=TCS.\n\nfree_cash_flow for year 2016 was 210960.000000 and for year 2017 was 272120.000000 and for year 2018 was 269290.000000 and for year 2019 was 306460.000000 and for year 2020 was 349070.000000 and for year 2021 was 415210.000000 and for year 2022 was 424320.000000 and for year 2023 was 444970.000000 and for year 2024 was 465400.000000 and for year 2025 was 528880.000000 for company=TCS.\n\neffective_tax_rate for year 2016 was 0.239146% and for year 2017 was 0.240817% and for year 2018 was 0.247640% and for year 2019 was 0.243618% and for year 2020 was 0.233496% and for year 2021 was 0.250733% and for year 2022 was 0.257253% and for year 2023 was 0.257753% and for year 2024 was 0.253844% and for year 2025 was 0.254264% for company=TCS.\n\ntax_burden for year 2016 was 0.239146% and for year 2017 was 0.240817% and for year 2018 was 0.247640% and for year 2019 was 0.243618% and for year 2020 was 0.233496% and for year 2021 was 0.250733% and for year 2022 was 0.257253% and for year 2023 was 0.257753% and for year 2024 was 0.253844% and for year 2025 was 0.254264% for company=TCS.\n\ninventory_turnover for year 2016 was 3459.250000 and for year 2017 was 3068.047619 and for year 2018 was 2657.538462 and for year 2019 was 8051.600000 and for year 2020 was 17571.400000 and for year 2021 was 11659.500000 and for year 2022 was 5435.850000 and for year 2023 was 4621.535714 and for year 2024 was 5136.892857 and for year 2025 was 7496.952381 for company=TCS.\n\nfixed_asset_turnover for year 2016 was 9.333047 and for year 2017 was 10.171236 and for year 2018 was 10.710284 and for year 2019 was 12.877000 and for year 2020 was 7.910337 and for year 2021 was 8.346993 and for year 2022 was 9.775886 and for year 2023 was 11.851241 and for year 2024 was 12.795761 and for year 2025 was 11.712647 for company=TCS.\n\nnet_profit_margin for year 2016 was 0.223386% and for year 2017 was 0.222852% and for year 2018 was 0.209790% and for year 2019 was 0.214880% and for year 2020 was 0.206054% and for year 2021 was 0.197531% and for year 2022 was 0.199876% and for year 2023 was 0.186939% and for year 2024 was 0.190574% and for year 2025 was 0.190162% for company=TCS.\n\nretention_ratio for year 2016 was 1.329337% and for year 2017 was 1.348511% and for year 2018 was 1.359483% and for year 2019 was 1.320444% and for year 2020 was 1.986271% and for year 2021 was 1.334567% and for year 2022 was 1.347457% and for year 2023 was 1.981019% and for year 2024 was 1.547552% and for year 2025 was 1.924021% for company=TCS.\n\nreturn_on_equity for year 2016 was 0.339787% and for year 2017 was 0.303638% and for year 2018 was 0.301953% and for year 2019 was 0.350082% and for year 2020 was 0.381597% and for year 2021 was 0.372296% and for year 2022 was 0.426585% and for year 2023 was 0.462108% and for year 2024 was 0.502721% and for year 2025 was 0.506970% for company=TCS.\n\nminority_interest_to_net_profit for year 2016 was 0.014627% and for year 2017 was 0.013922% and for year 2018 was 0.015566% and for year 2019 was 0.014394% and for year 2020 was 0.019264% and for year 2021 was 0.020814% and for year 2022 was 0.018447% and for year 2023 was 0.018554% and for year 2024 was 0.018080% and for year 2025 was 0.020905% for company=TCS.\n\nebitda_margin for year 2016 was 0.282587% and for year 2017 was 0.274045% and for year 2018 was 0.264264% and for year 2019 was 0.269775% and for year 2020 was 0.260225% and for year 2021 was 0.274381% and for year 2022 was 0.268511% and for year 2023 was 0.255591% and for year 2024 was 0.259783% and for year 2025 was 0.256889% for company=TCS.\n\nshareholder_funds_to_total_assets for year 2016 was 0.801686% and for year 2017 was 0.838531% and for year 2018 was 0.804640% and for year 2019 was 0.782118% and for year 2020 was 0.700990% and for year 2021 was 0.666172% and for year 2022 was 0.634891% and for year 2023 was 0.634914% and for year 2024 was 0.623555% and for year 2025 was 0.599960% for company=TCS.\n\ncash_return_on_equity for year 2016 was 0.267532% and for year 2017 was 0.291326% and for year 2018 was 0.293078% and for year 2019 was 0.318057% and for year 2020 was 0.381940% and for year 2021 was 0.445447% and for year 2022 was 0.444639% and for year 2023 was 0.460112% and for year 2024 was 0.485529% and for year 2025 was 0.510677% for company=TCS.\n\nasset_efficiency_ratio for year 2016 was 1.219426 and for year 2017 was 1.142506 and for year 2018 was 1.158124 and for year 2019 was 1.274223 and for year 2020 was 1.298183 and for year 2021 was 1.255569 and for year 2022 was 1.355018 and for year 2023 was 1.569484 and for year 2024 was 1.644893 and for year 2025 was 1.599484 for company=TCS.\n\nprice_to_book_value for year 2016 was 6.769551 and for year 2017 was 5.534466 and for year 2018 was 6.376882 and for year 2019 was 8.354922 and for year 2020 was 7.324240 and for year 2021 was 13.440386 and for year 2022 was 15.308018 and for year 2023 was 12.861773 and for year 2024 was 15.358076 and for year 2025 was 13.623551 for company=TCS.\n\nebit_margin for year 2016 was 0.265210% and for year 2017 was 0.257201% and for year 2018 was 0.247904% and for year 2019 was 0.255737% and for year 2020 was 0.245819% and for year 2021 was 0.258757% and for year 2022 was 0.252636% and for year 2023 was 0.240564% and for year 2024 was 0.246213% and for year 2025 was 0.243487% for company=TCS.\n\ncash_flow_margin for year 2016 was 0.175883% and for year 2017 was 0.213816% and for year 2018 was 0.203625% and for year 2019 was 0.195223% and for year 2020 was 0.206239% and for year 2021 was 0.236342% and for year 2022 was 0.208335% and for year 2023 was 0.186132% and for year 2024 was 0.184057% and for year 2025 was 0.191553% for company=TCS.\n\nearnings_per_share for year 2016 was 61.584917 and for year 2017 was 66.708112 and for year 2018 was 67.455467 and for year 2019 was 83.871655 and for year 2020 was 86.184842 and for year 2021 was 87.669974 and for year 2022 was 104.744336 and for year 2023 was 115.184062 and for year 2024 was 126.884276 and for year 2025 was 134.194743 for company=TCS.\n\ncash_ratio for year 2016 was 0.404329 and for year 2017 was 0.247864 and for year 2018 was 0.273895 and for year 2019 was 0.327115 and for year 2020 was 0.319512 and for year 2021 was 0.200791 and for year 2022 was 0.294869 and for year 2023 was 0.163529 and for year 2024 was 0.195558 and for year 2025 was 0.157393 for company=TCS.\n\ndividend_payout_coverage for year 2016 was -2.390717 and for year 2017 was -2.753002 and for year 2018 was -2.700022 and for year 2019 was -2.835201 and for year 2020 was -1.014829 and for year 2021 was -3.576221 and for year 2022 was -2.999850 and for year 2023 was -1.014947 and for year 2024 was -1.763854 and for year 2025 was -1.090139 for company=TCS.\n\nnet_profit_to_cash_flow_conversion for year 2016 was 1.270082 and for year 2017 was 1.042263 and for year 2018 was 1.030279 and for year 2019 was 1.100689 and for year 2020 was 0.999104 and for year 2021 was 0.835782 and for year 2022 was 0.959398 and for year 2023 was 1.004337 and for year 2024 was 1.035410 and for year 2025 was 0.992741 for company=TCS.\n\nother_expense_ratio for year 2016 was 0.225356% and for year 2017 was 0.196633% and for year 2018 was 0.190814% and for year 2019 was 0.194527% and for year 2020 was 0.194401% and for year 2021 was 0.173100% and for year 2022 was 0.180403% and for year 2023 was 0.185480% and for year 2024 was 0.156704% and for year 2025 was 0.139901% for company=TCS.\n\nbook_value_per_share for year 2016 was 181.245401 and for year 2017 was 219.696009 and for year 2018 was 223.397587 and for year 2019 was 239.577337 and for year 2020 was 225.852788 and for year 2021 was 235.484307 and for year 2022 was 245.541253 and for year 2023 was 249.258014 and for year 2024 was 252.394903 and for year 2025 was 264.699704 for company=TCS.\n\nretained_earnings_ratio for year 2016 was 1.329337% and for year 2017 was 1.348511% and for year 2018 was 1.359483% and for year 2019 was 1.320444% and for year 2020 was 1.986271% and for year 2021 was 1.334567% and for year 2022 was 1.347457% and for year 2023 was 1.981019% and for year 2024 was 1.547552% and for year 2025 was 1.924021% for company=TCS.\n\ngross_profit_margin for year 2016 was 0.490566% and for year 2017 was 0.453834% and for year 2018 was 0.438718% and for year 2019 was 0.450264% and for year 2020 was 0.440219% and for year 2021 was 0.431857% and for year 2022 was 0.433039% and for year 2023 was 0.426044% and for year 2024 was 0.402917% and for year 2025 was 0.383387% for company=TCS.\n\nreturn_on_assets for year 2016 was 0.272403% and for year 2017 was 0.254610% and for year 2018 was 0.242963% and for year 2019 was 0.273805% and for year 2020 was 0.267496% and for year 2021 was 0.248014% and for year 2022 was 0.270835% and for year 2023 was 0.293399% and for year 2024 was 0.313474% and for year 2025 was 0.304162% for company=TCS.\n\ndividend_payout_ratio for year 2016 was -0.329337% and for year 2017 was -0.348511% and for year 2018 was -0.359483% and for year 2019 was -0.320444% and for year 2020 was -0.986271% and for year 2021 was -0.334567% and for year 2022 was -0.347457% and for year 2023 was -0.981019% and for year 2024 was -0.547552% and for year 2025 was -0.924021% for company=TCS.\n\ndividend_yield for year 2016 was -1.653056% and for year 2017 was -1.912040% and for year 2018 was -1.702191% and for year 2019 was -1.342699% and for year 2020 was -5.138532% and for year 2021 was -0.926744% and for year 2022 was -0.968253% and for year 2023 was -3.524680% and for year 2024 was -1.792320% and for year 2025 was -3.438536% for company=TCS.\n\noperating_profit_margin for year 2016 was 0.265210% and for year 2017 was 0.257201% and for year 2018 was 0.247904% and for year 2019 was 0.255737% and for year 2020 was 0.245819% and for year 2021 was 0.258757% and for year 2022 was 0.252636% and for year 2023 was 0.240564% and for year 2024 was 0.246213% and for year 2025 was 0.243487% for company=TCS.\n\nreserves_to_equity for year 2016 was 339.324873 and for year 2017 was 415.913706 and for year 2018 was 434.628272 and for year 2019 was 231.704000 and for year 2020 was 215.482667 and for year 2021 was 223.129730 and for year 2022 was 233.898907 and for year 2023 was 236.814208 and for year 2024 was 238.748619 and for year 2025 was 248.715470 for company=TCS.\n\n--- Data for JIOFIN ---\ndebt_to_assets for year 2016 was 0.701234% and for year 2017 was 0.526917% and for year 2018 was 0.454752% and for year 2019 was 0.583680% and for year 2020 was 0.593576% and for year 2021 was 0.000116% and for year 2022 was 0.000040% and for year 2023 was 0.007043% and for year 2024 was 0.039453% and for year 2025 was 0.075001% for company=JIOFIN.\n\nreceivables_turnover for year 2016 was -0.510736 and for year 2017 was 2.632721 and for year 2018 was 7.136335 and for year 2019 was -46.133333 and for year 2020 was 0.050540 and for year 2021 was 0.094619 and for year 2022 was 0.091959 and for year 2023 was 1.257970 and for year 2024 was 10.625815 and for year 2025 was 0.198437 for company=JIOFIN.\n\ndepreciation_to_net_profit for year 2016 was N/A and for year 2017 was N/A and for year 2018 was N/A and for year 2019 was N/A and for year 2020 was N/A and for year 2021 was N/A and for year 2022 was N/A and for year 2023 was N/A and for year 2024 was 0.013412% and for year 2025 was 0.013965% for company=JIOFIN.\n\ncurrent_ratio for year 2016 was 1.298290 and for year 2017 was 1.758325 and for year 2018 was 2.226069 and for year 2019 was 1.753035 and for year 2020 was 1.917959 and for year 2021 was 29222.857143 and for year 2022 was 29601.714286 and for year 2023 was 20.942348 and for year 2024 was 111.415587 and for year 2025 was 4.766535 for company=JIOFIN.\n\ncapex_to_operating_cash_flow for year 2016 was N/A and for year 2017 was N/A and for year 2018 was N/A and for year 2019 was N/A and for year 2020 was N/A and for year 2021 was N/A and for year 2022 was N/A and for year 2023 was N/A and for year 2024 was 0.001166% and for year 2025 was 0.004145% for company=JIOFIN.\n\noperating_cash_flow_ratio for year 2016 was -0.271511 and for year 2017 was 0.751737 and for year 2018 was 0.099695 and for year 2019 was -0.013104 and for year 2020 was -1.487042 and for year 2021 was 34773.285714 and for year 2022 was 1757.714286 and for year 2023 was 2.564266 and for year 2024 was -4.389544 and for year 2025 was -3.338793 for company=JIOFIN.\n\nnet_working_capital for year 2016 was 10103.100000 and for year 2017 was 13904.800000 and for year 2018 was 21300.500000 and for year 2019 was 17612.800000 and for year 2020 was 22758.500000 and for year 2021 was 20455.300000 and for year 2022 was 20720.500000 and for year 2023 was 159810.000000 and for year 2024 was 170437.500000 and for year 2025 was 113752.000000 for company=JIOFIN.\n\nprice_to_earnings_ratio for year 2016 was N/A and for year 2017 was N/A and for year 2018 was N/A and for year 2019 was N/A and for year 2020 was N/A and for year 2021 was N/A and for year 2022 was N/A and for year 2023 was N/A and for year 2024 was 140.069170 and for year 2025 was 89.631821 for company=JIOFIN.\n\nasset_turnover for year 2016 was -0.025608 and for year 2017 was 0.125179 and for year 2018 was 0.058200 and for year 2019 was -0.086074 and for year 2020 was 0.046475 and for year 2021 was 0.089607 and for year 2022 was 0.082585 and for year 2023 was 0.000450 and for year 2024 was 0.012712 and for year 2025 was 0.014942 for company=JIOFIN.\n\ncash_earnings_per_share for year 2016 was N/A and for year 2017 was N/A and for year 2018 was N/A and for year 2019 was N/A and for year 2020 was N/A and for year 2021 was N/A and for year 2022 was N/A and for year 2023 was N/A and for year 2024 was 2.559410 and for year 2025 was 2.573720 for company=JIOFIN.\n\ncash_return_on_assets for year 2016 was -0.190393% and for year 2017 was 0.396103% and for year 2018 was 0.044785% and for year 2019 was -0.007475% and for year 2020 was -0.773586% and for year 2021 was 1.181485% and for year 2022 was 0.055221% and for year 2023 was 0.017880% and for year 2024 was -0.004677% and for year 2025 was -0.075525% for company=JIOFIN.\n\nreturn_on_investment for year 2016 was N/A and for year 2017 was N/A and for year 2018 was N/A and for year 2019 was N/A and for year 2020 was N/A and for year 2021 was N/A and for year 2022 was N/A and for year 2023 was N/A and for year 2024 was N/A and for year 2025 was 0.012955% for company=JIOFIN.\n\nequity_ratio for year 2016 was 0.298766% and for year 2017 was 0.473083% and for year 2018 was 0.545248% and for year 2019 was 0.416320% and for year 2020 was 0.406424% and for year 2021 was 0.999884% and for year 2022 was 0.999960% and for year 2023 was 0.992957% and for year 2024 was 0.960547% and for year 2025 was 0.924999% for company=JIOFIN.\n\ndepreciation_to_sales for year 2016 was N/A and for year 2017 was N/A and for year 2018 was N/A and for year 2019 was N/A and for year 2020 was N/A and for year 2021 was N/A and for year 2022 was N/A and for year 2023 was N/A and for year 2024 was 0.011686% and for year 2025 was 0.011289% for company=JIOFIN.\n\nnet_income_to_operating_cf_ratio for year 2016 was 0.327128 and for year 2017 was 0.239386 and for year 2018 was 1.395439 and for year 2019 was 13.105383 and for year 2020 was -0.062369 and for year 2021 was 0.050556 and for year 2022 was 1.365735 and for year 2023 was 0.015208 and for year 2024 was -2.368095 and for year 2025 was -0.159925 for company=JIOFIN.\n\nother_income_to_sales for year 2016 was -0.003396% and for year 2017 was 1.611924% and for year 2018 was -0.224365% and for year 2019 was 0.132835% and for year 2020 was 0.305883% and for year 2021 was N/A and for year 2022 was 0.192381% and for year 2023 was 0.194622% and for year 2024 was -0.001113% and for year 2025 was 0.377922% for company=JIOFIN.\n\nworking_capital_to_total_assets for year 2016 was 0.209171% and for year 2017 was 0.399575% and for year 2018 was 0.550776% and for year 2019 was 0.429561% and for year 2020 was 0.477539% and for year 2021 was 0.992865% and for year 2022 was 0.929954% and for year 2023 was 0.139050% and for year 2024 was 0.117654% and for year 2025 was 0.085201% for company=JIOFIN.\n\nfinancial_leverage for year 2016 was 3.347103 and for year 2017 was 2.113796 and for year 2018 was 1.834028 and for year 2019 was 2.402000 and for year 2020 was 2.460486 and for year 2021 was 1.000117 and for year 2022 was 1.000040 and for year 2023 was 1.007093 and for year 2024 was 1.041074 and for year 2025 was 1.081083 for company=JIOFIN.\n\nfree_cash_flow for year 2016 was N/A and for year 2017 was N/A and for year 2018 was N/A and for year 2019 was N/A and for year 2020 was N/A and for year 2021 was N/A and for year 2022 was N/A and for year 2023 was N/A and for year 2024 was -6767.800000 and for year 2025 was -100415.900000 for company=JIOFIN.\n\neffective_tax_rate for year 2016 was -0.020247% and for year 2017 was 0.222136% and for year 2018 was -0.149864% and for year 2019 was -0.088947% and for year 2020 was -0.186297% and for year 2021 was -0.038370% and for year 2022 was 0.049924% and for year 2023 was 0.366640% and for year 2024 was 0.179632% and for year 2025 was 0.171718% for company=JIOFIN.\n\ntax_burden for year 2016 was -0.020247% and for year 2017 was 0.222136% and for year 2018 was -0.149864% and for year 2019 was -0.088947% and for year 2020 was -0.186297% and for year 2021 was -0.038370% and for year 2022 was 0.049924% and for year 2023 was 0.366640% and for year 2024 was 0.179632% and for year 2025 was 0.171718% for company=JIOFIN.\n\nfixed_asset_turnover for year 2016 was N/A and for year 2017 was N/A and for year 2018 was N/A and for year 2019 was N/A and for year 2020 was N/A and for year 2021 was N/A and for year 2022 was N/A and for year 2023 was 1.306953 and for year 2024 was 58.892229 and for year 2025 was 50.812787 for company=JIOFIN.\n\ndebt_to_equity for year 2016 was N/A and for year 2017 was N/A and for year 2018 was N/A and for year 2019 was N/A and for year 2020 was N/A and for year 2021 was N/A and for year 2022 was N/A and for year 2023 was N/A and for year 2024 was N/A and for year 2025 was 0.007962 for company=JIOFIN.\n\nnet_profit_margin for year 2016 was 2.432129% and for year 2017 was 0.757489% and for year 2018 was 1.073796% and for year 2019 was 1.138162% and for year 2020 was 1.038151% and for year 2021 was 0.666594% and for year 2022 was 0.913211% and for year 2023 was 0.604566% and for year 2024 was 0.871299% and for year 2025 was 0.808352% for company=JIOFIN.\n\nreturn_on_equity for year 2016 was -0.208467% and for year 2017 was 0.200434% and for year 2018 was 0.114617% and for year 2019 was -0.235315% and for year 2020 was 0.118714% and for year 2021 was 0.059738% and for year 2022 was 0.075421% and for year 2023 was 0.000274% and for year 2024 was 0.011531% and for year 2025 was 0.013058% for company=JIOFIN.\n\nshareholder_funds_to_total_assets for year 2016 was 0.298766% and for year 2017 was 0.473083% and for year 2018 was 0.545248% and for year 2019 was 0.416320% and for year 2020 was 0.406424% and for year 2021 was 0.999884% and for year 2022 was 0.999960% and for year 2023 was 0.992957% and for year 2024 was 0.960547% and for year 2025 was 0.924999% for company=JIOFIN.\n\ncash_return_on_equity for year 2016 was -0.637264% and for year 2017 was 0.837282% and for year 2018 was 0.082137% and for year 2019 was -0.017956% and for year 2020 was -1.903399% and for year 2021 was 1.181622% and for year 2022 was 0.055224% and for year 2023 was 0.018006% and for year 2024 was -0.004869% and for year 2025 was -0.081649% for company=JIOFIN.\n\nasset_efficiency_ratio for year 2016 was -0.025608 and for year 2017 was 0.125179 and for year 2018 was 0.058200 and for year 2019 was -0.086074 and for year 2020 was 0.046475 and for year 2021 was 0.089607 and for year 2022 was 0.082585 and for year 2023 was 0.000450 and for year 2024 was 0.012712 and for year 2025 was 0.014942 for company=JIOFIN.\n\nprice_to_book_value for year 2016 was N/A and for year 2017 was N/A and for year 2018 was N/A and for year 2019 was N/A and for year 2020 was N/A and for year 2021 was N/A and for year 2022 was N/A and for year 2023 was N/A and for year 2024 was 1.615176 and for year 2025 was 1.170392 for company=JIOFIN.\n\nebit_margin for year 2016 was 2.378608% and for year 2017 was 0.975483% and for year 2018 was 0.948063% and for year 2019 was 1.031027% and for year 2020 was 0.875164% and for year 2021 was 0.664428% and for year 2022 was 0.961741% and for year 2023 was 0.892436% and for year 2024 was 0.828955% and for year 2025 was 0.760977% for company=JIOFIN.\n\ncash_flow_margin for year 2016 was 7.434797% and for year 2017 was 3.164298% and for year 2018 was 0.769504% and for year 2019 was 0.086847% and for year 2020 was -16.645221% and for year 2021 was 13.185255% and for year 2022 was 0.668659% and for year 2023 was 39.754305% and for year 2024 was -0.367933% and for year 2025 was -5.054559% for company=JIOFIN.\n\ncash_flow_to_debt for year 2016 was N/A and for year 2017 was N/A and for year 2018 was N/A and for year 2019 was N/A and for year 2020 was N/A and for year 2021 was N/A and for year 2022 was N/A and for year 2023 was N/A and for year 2024 was N/A and for year 2025 was -10.255373 for company=JIOFIN.\n\nearnings_per_share for year 2016 was -1504.150000 and for year 2017 was 1649.850000 and for year 2018 was 1208.450000 and for year 2019 was -2008.400000 and for year 2020 was 1149.700000 and for year 2021 was 615.300000 and for year 2022 was 840.200000 and for year 2023 was 156.250000 and for year 2024 was 2.525538 and for year 2025 was 2.538273 for company=JIOFIN.\n\ncash_ratio for year 2016 was 0.103740 and for year 2017 was 0.192166 and for year 2018 was 0.000391 and for year 2019 was 0.000449 and for year 2020 was 0.000198 and for year 2021 was 5.142857 and for year 2022 was 7.285714 and for year 2023 was 0.078766 and for year 2024 was 0.857023 and for year 2025 was 1.348240 for company=JIOFIN.\n\nnet_profit_to_cash_flow_conversion for year 2016 was 0.327128 and for year 2017 was 0.239386 and for year 2018 was 1.395439 and for year 2019 was 13.105383 and for year 2020 was -0.062369 and for year 2021 was 0.050556 and for year 2022 was 1.365735 and for year 2023 was 0.015208 and for year 2024 was -2.368095 and for year 2025 was -0.159925 for company=JIOFIN.\n\nother_expense_ratio for year 2016 was -0.045760% and for year 2017 was 0.023783% and for year 2018 was 0.050515% and for year 2019 was -0.029837% and for year 2020 was 0.124836% and for year 2021 was 0.335572% and for year 2022 was 0.038259% and for year 2023 was 0.107564% and for year 2024 was 0.171045% and for year 2025 was 0.239023% for company=JIOFIN.\n\nbook_value_per_share for year 2016 was 7215.300000 and for year 2017 was 8231.400000 and for year 2018 was 10543.350000 and for year 2019 was 8534.950000 and for year 2020 was 9684.650000 and for year 2021 was 10299.950000 and for year 2022 was 11140.150000 and for year 2023 was 570601.700000 and for year 2024 was 219.016354 and for year 2025 was 194.387811 for company=JIOFIN.\n\ngross_profit_margin for year 2016 was 2.332848% and for year 2017 was 0.999265% and for year 2018 was 0.998578% and for year 2019 was 1.001190% and for year 2020 was 1.000000% and for year 2021 was 1.000000% and for year 2022 was 1.000000% and for year 2023 was 1.000000% and for year 2024 was 1.000000% and for year 2025 was 1.000000% for company=JIOFIN.\n\nreturn_on_assets for year 2016 was -0.062283% and for year 2017 was 0.094822% and for year 2018 was 0.062495% and for year 2019 was -0.097966% and for year 2020 was 0.048248% and for year 2021 was 0.059731% and for year 2022 was 0.075418% and for year 2023 was 0.000272% and for year 2024 was 0.011076% and for year 2025 was 0.012078% for company=JIOFIN.\n\noperating_profit_margin for year 2016 was 2.378608% and for year 2017 was 0.975483% and for year 2018 was 0.948063% and for year 2019 was 1.031027% and for year 2020 was 0.875164% and for year 2021 was 0.664428% and for year 2022 was 0.961741% and for year 2023 was 0.892436% and for year 2024 was 0.828955% and for year 2025 was 0.760977% for company=JIOFIN.\n\nreserves_to_equity for year 2016 was 276.400990 and for year 2017 was 326.845494 and for year 2018 was 605.910891 and for year 2019 was 407.059406 and for year 2020 was 520.891089 and for year 2021 was 581.811881 and for year 2022 was 665.000000 and for year 2023 was 5189.326733 and for year 2024 was 1.902345 and for year 2025 was N/A for company=JIOFIN.",
                            "context_sources": {
                                "sql_tables_retrieved": [
                                    "balance_sheet",
                                    "profit_and_loss",
                                    "cashflow",
                                    "financial_ratios"
                                ],
                                "total_context_length": 115682,
                                "vector_context_length": 4847
                            },
                            "resolved_companies": [
                                {
                                    "aliases": [
                                        "tata consultancy services",
                                        "tcs"
                                    ],
                                    "full_name": "Tata Consultancy Services Limited",
                                    "sector": "Information Technology",
                                    "ticker": "TCS"
                                },
                                {
                                    "aliases": [
                                        "jio financial services",
                                        "jio financial",
                                        "jiofin"
                                    ],
                                    "full_name": "Jio Financial Services Limited",
                                    "sector": "Financial Services",
                                    "ticker": "JIOFIN"
                                }
                            ]
                        },
                        "company_overviews": [],
                        "shareholding_data": [],
                        "chart_data": {
                            "plotly_json": "{\"data\":[{\"mode\":\"lines+markers\",\"name\":\"Tata Consultancy Services Limited - net_profit_margin\",\"x\":[\"Mar 2021\",\"Mar 2022\",\"Mar 2023\"],\"y\":[0.197531,0.199876,0.186939],\"type\":\"scatter\"},{\"mode\":\"lines+markers\",\"name\":\"Tata Consultancy Services Limited - gross_profit_margin\",\"x\":[\"Mar 2021\",\"Mar 2022\",\"Mar 2023\"],\"y\":[0.431857,0.433039,0.426044],\"type\":\"scatter\"},{\"mode\":\"lines+markers\",\"name\":\"Tata Consultancy Services Limited - return_on_equity\",\"x\":[\"Mar 2021\",\"Mar 2022\",\"Mar 2023\"],\"y\":[0.372296,0.426585,0.462108],\"type\":\"scatter\"},{\"mode\":\"lines+markers\",\"name\":\"Tata Consultancy Services Limited - return_on_assets\",\"x\":[\"Mar 2021\",\"Mar 2022\",\"Mar 2023\"],\"y\":[0.248014,0.270835,0.293399],\"type\":\"scatter\"},{\"mode\":\"lines+markers\",\"name\":\"Tata Consultancy Services Limited - current_ratio\",\"x\":[\"Mar 2021\",\"Mar 2022\",\"Mar 2023\"],\"y\":[2.906749,2.557437,2.531567],\"type\":\"scatter\"},{\"mode\":\"lines+markers\",\"name\":\"Reliance Jio Financial Services - net_profit_margin\",\"x\":[\"Mar 2021\",\"Mar 2022\",\"Mar 2023\"],\"y\":[0.666594,0.913211,0.604566],\"type\":\"scatter\"},{\"mode\":\"lines+markers\",\"name\":\"Reliance Jio Financial Services - gross_profit_margin\",\"x\":[\"Mar 2021\",\"Mar 2022\",\"Mar 2023\"],\"y\":[1.0,1.0,1.0],\"type\":\"scatter\"},{\"mode\":\"lines+markers\",\"name\":\"Reliance Jio Financial Services - return_on_equity\",\"x\":[\"Mar 2021\",\"Mar 2022\",\"Mar 2023\"],\"y\":[0.059738,0.075421,0.000274],\"type\":\"scatter\"},{\"mode\":\"lines+markers\",\"name\":\"Reliance Jio Financial Services - return_on_assets\",\"x\":[\"Mar 2021\",\"Mar 2022\",\"Mar 2023\"],\"y\":[0.059731,0.075418,0.000272],\"type\":\"scatter\"},{\"mode\":\"lines+markers\",\"name\":\"Reliance Jio Financial Services - current_ratio\",\"x\":[\"Mar 2021\",\"Mar 2022\",\"Mar 2023\"],\"y\":[29222.857143,29601.714286,20.942348],\"type\":\"scatter\"}],\"layout\":{\"template\":{\"data\":{\"histogram2dcontour\":[{\"type\":\"histogram2dcontour\",\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"},\"colorscale\":[[0.0,\"#0d0887\"],[0.1111111111111111,\"#46039f\"],[0.2222222222222222,\"#7201a8\"],[0.3333333333333333,\"#9c179e\"],[0.4444444444444444,\"#bd3786\"],[0.5555555555555556,\"#d8576b\"],[0.6666666666666666,\"#ed7953\"],[0.7777777777777778,\"#fb9f3a\"],[0.8888888888888888,\"#fdca26\"],[1.0,\"#f0f921\"]]}],\"choropleth\":[{\"type\":\"choropleth\",\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}}],\"histogram2d\":[{\"type\":\"histogram2d\",\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"},\"colorscale\":[[0.0,\"#0d0887\"],[0.1111111111111111,\"#46039f\"],[0.2222222222222222,\"#7201a8\"],[0.3333333333333333,\"#9c179e\"],[0.4444444444444444,\"#bd3786\"],[0.5555555555555556,\"#d8576b\"],[0.6666666666666666,\"#ed7953\"],[0.7777777777777778,\"#fb9f3a\"],[0.8888888888888888,\"#fdca26\"],[1.0,\"#f0f921\"]]}],\"heatmap\":[{\"type\":\"heatmap\",\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"},\"colorscale\":[[0.0,\"#0d0887\"],[0.1111111111111111,\"#46039f\"],[0.2222222222222222,\"#7201a8\"],[0.3333333333333333,\"#9c179e\"],[0.4444444444444444,\"#bd3786\"],[0.5555555555555556,\"#d8576b\"],[0.6666666666666666,\"#ed7953\"],[0.7777777777777778,\"#fb9f3a\"],[0.8888888888888888,\"#fdca26\"],[1.0,\"#f0f921\"]]}],\"contourcarpet\":[{\"type\":\"contourcarpet\",\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}}],\"contour\":[{\"type\":\"contour\",\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"},\"colorscale\":[[0.0,\"#0d0887\"],[0.1111111111111111,\"#46039f\"],[0.2222222222222222,\"#7201a8\"],[0.3333333333333333,\"#9c179e\"],[0.4444444444444444,\"#bd3786\"],[0.5555555555555556,\"#d8576b\"],[0.6666666666666666,\"#ed7953\"],[0.7777777777777778,\"#fb9f3a\"],[0.8888888888888888,\"#fdca26\"],[1.0,\"#f0f921\"]]}],\"surface\":[{\"type\":\"surface\",\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"},\"colorscale\":[[0.0,\"#0d0887\"],[0.1111111111111111,\"#46039f\"],[0.2222222222222222,\"#7201a8\"],[0.3333333333333333,\"#9c179e\"],[0.4444444444444444,\"#bd3786\"],[0.5555555555555556,\"#d8576b\"],[0.6666666666666666,\"#ed7953\"],[0.7777777777777778,\"#fb9f3a\"],[0.8888888888888888,\"#fdca26\"],[1.0,\"#f0f921\"]]}],\"mesh3d\":[{\"type\":\"mesh3d\",\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}}],\"scatter\":[{\"fillpattern\":{\"fillmode\":\"overlay\",\"size\":10,\"solidity\":0.2},\"type\":\"scatter\"}],\"parcoords\":[{\"type\":\"parcoords\",\"line\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}}}],\"scatterpolargl\":[{\"type\":\"scatterpolargl\",\"marker\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}}}],\"bar\":[{\"error_x\":{\"color\":\"#2a3f5f\"},\"error_y\":{\"color\":\"#2a3f5f\"},\"marker\":{\"line\":{\"color\":\"#E5ECF6\",\"width\":0.5},\"pattern\":{\"fillmode\":\"overlay\",\"size\":10,\"solidity\":0.2}},\"type\":\"bar\"}],\"scattergeo\":[{\"type\":\"scattergeo\",\"marker\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}}}],\"scatterpolar\":[{\"type\":\"scatterpolar\",\"marker\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}}}],\"histogram\":[{\"marker\":{\"pattern\":{\"fillmode\":\"overlay\",\"size\":10,\"solidity\":0.2}},\"type\":\"histogram\"}],\"scattergl\":[{\"type\":\"scattergl\",\"marker\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}}}],\"scatter3d\":[{\"type\":\"scatter3d\",\"line\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}},\"marker\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}}}],\"scattermap\":[{\"type\":\"scattermap\",\"marker\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}}}],\"scattermapbox\":[{\"type\":\"scattermapbox\",\"marker\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}}}],\"scatterternary\":[{\"type\":\"scatterternary\",\"marker\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}}}],\"scattercarpet\":[{\"type\":\"scattercarpet\",\"marker\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}}}],\"carpet\":[{\"aaxis\":{\"endlinecolor\":\"#2a3f5f\",\"gridcolor\":\"white\",\"linecolor\":\"white\",\"minorgridcolor\":\"white\",\"startlinecolor\":\"#2a3f5f\"},\"baxis\":{\"endlinecolor\":\"#2a3f5f\",\"gridcolor\":\"white\",\"linecolor\":\"white\",\"minorgridcolor\":\"white\",\"startlinecolor\":\"#2a3f5f\"},\"type\":\"carpet\"}],\"table\":[{\"cells\":{\"fill\":{\"color\":\"#EBF0F8\"},\"line\":{\"color\":\"white\"}},\"header\":{\"fill\":{\"color\":\"#C8D4E3\"},\"line\":{\"color\":\"white\"}},\"type\":\"table\"}],\"barpolar\":[{\"marker\":{\"line\":{\"color\":\"#E5ECF6\",\"width\":0.5},\"pattern\":{\"fillmode\":\"overlay\",\"size\":10,\"solidity\":0.2}},\"type\":\"barpolar\"}],\"pie\":[{\"automargin\":true,\"type\":\"pie\"}]},\"layout\":{\"autotypenumbers\":\"strict\",\"colorway\":[\"#636efa\",\"#EF553B\",\"#00cc96\",\"#ab63fa\",\"#FFA15A\",\"#19d3f3\",\"#FF6692\",\"#B6E880\",\"#FF97FF\",\"#FECB52\"],\"font\":{\"color\":\"#2a3f5f\"},\"hovermode\":\"closest\",\"hoverlabel\":{\"align\":\"left\"},\"paper_bgcolor\":\"white\",\"plot_bgcolor\":\"#E5ECF6\",\"polar\":{\"bgcolor\":\"#E5ECF6\",\"angularaxis\":{\"gridcolor\":\"white\",\"linecolor\":\"white\",\"ticks\":\"\"},\"radialaxis\":{\"gridcolor\":\"white\",\"linecolor\":\"white\",\"ticks\":\"\"}},\"ternary\":{\"bgcolor\":\"#E5ECF6\",\"aaxis\":{\"gridcolor\":\"white\",\"linecolor\":\"white\",\"ticks\":\"\"},\"baxis\":{\"gridcolor\":\"white\",\"linecolor\":\"white\",\"ticks\":\"\"},\"caxis\":{\"gridcolor\":\"white\",\"linecolor\":\"white\",\"ticks\":\"\"}},\"coloraxis\":{\"colorbar\":{\"outlinewidth\":0,\"ticks\":\"\"}},\"colorscale\":{\"sequential\":[[0.0,\"#0d0887\"],[0.1111111111111111,\"#46039f\"],[0.2222222222222222,\"#7201a8\"],[0.3333333333333333,\"#9c179e\"],[0.4444444444444444,\"#bd3786\"],[0.5555555555555556,\"#d8576b\"],[0.6666666666666666,\"#ed7953\"],[0.7777777777777778,\"#fb9f3a\"],[0.8888888888888888,\"#fdca26\"],[1.0,\"#f0f921\"]],\"sequentialminus\":[[0.0,\"#0d0887\"],[0.1111111111111111,\"#46039f\"],[0.2222222222222222,\"#7201a8\"],[0.3333333333333333,\"#9c179e\"],[0.4444444444444444,\"#bd3786\"],[0.5555555555555556,\"#d8576b\"],[0.6666666666666666,\"#ed7953\"],[0.7777777777777778,\"#fb9f3a\"],[0.8888888888888888,\"#fdca26\"],[1.0,\"#f0f921\"]],\"diverging\":[[0,\"#8e0152\"],[0.1,\"#c51b7d\"],[0.2,\"#de77ae\"],[0.3,\"#f1b6da\"],[0.4,\"#fde0ef\"],[0.5,\"#f7f7f7\"],[0.6,\"#e6f5d0\"],[0.7,\"#b8e186\"],[0.8,\"#7fbc41\"],[0.9,\"#4d9221\"],[1,\"#276419\"]]},\"xaxis\":{\"gridcolor\":\"white\",\"linecolor\":\"white\",\"ticks\":\"\",\"title\":{\"standoff\":15},\"zerolinecolor\":\"white\",\"automargin\":true,\"zerolinewidth\":2},\"yaxis\":{\"gridcolor\":\"white\",\"linecolor\":\"white\",\"ticks\":\"\",\"title\":{\"standoff\":15},\"zerolinecolor\":\"white\",\"automargin\":true,\"zerolinewidth\":2},\"scene\":{\"xaxis\":{\"backgroundcolor\":\"#E5ECF6\",\"gridcolor\":\"white\",\"linecolor\":\"white\",\"showbackground\":true,\"ticks\":\"\",\"zerolinecolor\":\"white\",\"gridwidth\":2},\"yaxis\":{\"backgroundcolor\":\"#E5ECF6\",\"gridcolor\":\"white\",\"linecolor\":\"white\",\"showbackground\":true,\"ticks\":\"\",\"zerolinecolor\":\"white\",\"gridwidth\":2},\"zaxis\":{\"backgroundcolor\":\"#E5ECF6\",\"gridcolor\":\"white\",\"linecolor\":\"white\",\"showbackground\":true,\"ticks\":\"\",\"zerolinecolor\":\"white\",\"gridwidth\":2}},\"shapedefaults\":{\"line\":{\"color\":\"#2a3f5f\"}},\"annotationdefaults\":{\"arrowcolor\":\"#2a3f5f\",\"arrowhead\":0,\"arrowwidth\":1},\"geo\":{\"bgcolor\":\"white\",\"landcolor\":\"#E5ECF6\",\"subunitcolor\":\"white\",\"showland\":true,\"showlakes\":true,\"lakecolor\":\"white\"},\"title\":{\"x\":0.05},\"mapbox\":{\"style\":\"light\"}}},\"title\":{\"text\":\"Financial Ratio Comparison\"},\"xaxis\":{\"title\":{\"text\":\"Year\"}},\"yaxis\":{\"title\":{\"text\":\"Value\"}}}}"
                        },
                        "financial_data": [
                            [
                                {
                                    "company_name": "Tata Consultancy Services Limited",
                                    "ticker": "TCS",
                                    "company_number": 20,
                                    "headers": [
                                        "Ratio",
                                        "Mar 2021",
                                        "Mar 2022",
                                        "Mar 2023"
                                    ],
                                    "data": [
                                        {
                                            "Ratio": "debt_to_assets",
                                            "Mar 2021": "0.333828%",
                                            "Mar 2022": "0.365109%",
                                            "Mar 2023": "0.365086%"
                                        },
                                        {
                                            "Ratio": "depreciation_to_net_profit",
                                            "Mar 2021": "0.125347%",
                                            "Mar 2022": "0.120124%",
                                            "Mar 2023": "0.119154%"
                                        },
                                        {
                                            "Ratio": "receivables_turnover",
                                            "Mar 2021": "4.054554",
                                            "Mar 2022": "4.163316",
                                            "Mar 2023": "4.057189"
                                        },
                                        {
                                            "Ratio": "current_ratio",
                                            "Mar 2021": "2.906749",
                                            "Mar 2022": "2.557437",
                                            "Mar 2023": "2.531567"
                                        },
                                        {
                                            "Ratio": "sustainable_growth_rate",
                                            "Mar 2021": "0.496854%",
                                            "Mar 2022": "0.574806%",
                                            "Mar 2023": "0.915444%"
                                        },
                                        {
                                            "Ratio": "interest_coverage",
                                            "Mar 2021": "-66.690738",
                                            "Mar 2022": "-61.790816",
                                            "Mar 2023": "-69.623877"
                                        },
                                        {
                                            "Ratio": "capex_to_operating_cash_flow",
                                            "Mar 2021": "-0.070074%",
                                            "Mar 2022": "-0.062154%",
                                            "Mar 2023": "-0.060336%"
                                        },
                                        {
                                            "Ratio": "operating_cash_flow_ratio",
                                            "Mar 2021": "1.136056",
                                            "Mar 2022": "0.943284",
                                            "Mar 2023": "0.963428"
                                        },
                                        {
                                            "Ratio": "net_working_capital",
                                            "Mar 2021": "651250.000000",
                                            "Mar 2022": "659590.000000",
                                            "Mar 2023": "667120.000000"
                                        },
                                        {
                                            "Ratio": "price_to_earnings_ratio",
                                            "Mar 2021": "36.101300",
                                            "Mar 2022": "35.884995",
                                            "Mar 2023": "27.832844"
                                        },
                                        {
                                            "Ratio": "asset_turnover",
                                            "Mar 2021": "1.255569",
                                            "Mar 2022": "1.355018",
                                            "Mar 2023": "1.569484"
                                        },
                                        {
                                            "Ratio": "cash_earnings_per_share",
                                            "Mar 2021": "98.659133",
                                            "Mar 2022": "117.326665",
                                            "Mar 2023": "128.908748"
                                        },
                                        {
                                            "Ratio": "quick_ratio",
                                            "Mar 2021": "2.906514",
                                            "Mar 2022": "2.556964",
                                            "Mar 2023": "2.530924"
                                        },
                                        {
                                            "Ratio": "cash_return_on_assets",
                                            "Mar 2021": "0.296744%",
                                            "Mar 2022": "0.282297%",
                                            "Mar 2023": "0.292132%"
                                        },
                                        {
                                            "Ratio": "dividend_per_share",
                                            "Mar 2021": "-29.331459",
                                            "Mar 2022": "-36.394195",
                                            "Mar 2023": "-112.997732"
                                        },
                                        {
                                            "Ratio": "equity_ratio",
                                            "Mar 2021": "0.666172%",
                                            "Mar 2022": "0.634891%",
                                            "Mar 2023": "0.634914%"
                                        },
                                        {
                                            "Ratio": "dividend_coverage_ratio",
                                            "Mar 2021": "-3.576221",
                                            "Mar 2022": "-2.999850",
                                            "Mar 2023": "-1.014947"
                                        },
                                        {
                                            "Ratio": "depreciation_to_sales",
                                            "Mar 2021": "0.024760%",
                                            "Mar 2022": "0.024010%",
                                            "Mar 2023": "0.022275%"
                                        },
                                        {
                                            "Ratio": "net_income_to_operating_cf_ratio",
                                            "Mar 2021": "0.835782",
                                            "Mar 2022": "0.959398",
                                            "Mar 2023": "1.004337"
                                        },
                                        {
                                            "Ratio": "effective_interest_burden",
                                            "Mar 2021": "-0.014263%",
                                            "Mar 2022": "-0.015235%",
                                            "Mar 2023": "-0.013749%"
                                        },
                                        {
                                            "Ratio": "working_capital_to_total_assets",
                                            "Mar 2021": "0.498054%",
                                            "Mar 2022": "0.466095%",
                                            "Mar 2023": "0.464403%"
                                        },
                                        {
                                            "Ratio": "financial_leverage",
                                            "Mar 2021": "1.501114",
                                            "Mar 2022": "1.575073",
                                            "Mar 2023": "1.575017"
                                        },
                                        {
                                            "Ratio": "retained_earnings_to_equity",
                                            "Mar 2021": "0.496854%",
                                            "Mar 2022": "0.574806%",
                                            "Mar 2023": "0.915444%"
                                        },
                                        {
                                            "Ratio": "free_cash_flow",
                                            "Mar 2021": "415210.000000",
                                            "Mar 2022": "424320.000000",
                                            "Mar 2023": "444970.000000"
                                        },
                                        {
                                            "Ratio": "effective_tax_rate",
                                            "Mar 2021": "0.250733%",
                                            "Mar 2022": "0.257253%",
                                            "Mar 2023": "0.257753%"
                                        },
                                        {
                                            "Ratio": "tax_burden",
                                            "Mar 2021": "0.250733%",
                                            "Mar 2022": "0.257253%",
                                            "Mar 2023": "0.257753%"
                                        },
                                        {
                                            "Ratio": "inventory_turnover",
                                            "Mar 2021": "11659.500000",
                                            "Mar 2022": "5435.850000",
                                            "Mar 2023": "4621.535714"
                                        },
                                        {
                                            "Ratio": "fixed_asset_turnover",
                                            "Mar 2021": "8.346993",
                                            "Mar 2022": "9.775886",
                                            "Mar 2023": "11.851241"
                                        },
                                        {
                                            "Ratio": "net_profit_margin",
                                            "Mar 2021": "0.197531%",
                                            "Mar 2022": "0.199876%",
                                            "Mar 2023": "0.186939%"
                                        },
                                        {
                                            "Ratio": "retention_ratio",
                                            "Mar 2021": "1.334567%",
                                            "Mar 2022": "1.347457%",
                                            "Mar 2023": "1.981019%"
                                        },
                                        {
                                            "Ratio": "return_on_equity",
                                            "Mar 2021": "0.372296%",
                                            "Mar 2022": "0.426585%",
                                            "Mar 2023": "0.462108%"
                                        },
                                        {
                                            "Ratio": "minority_interest_to_net_profit",
                                            "Mar 2021": "0.020814%",
                                            "Mar 2022": "0.018447%",
                                            "Mar 2023": "0.018554%"
                                        },
                                        {
                                            "Ratio": "ebitda_margin",
                                            "Mar 2021": "0.274381%",
                                            "Mar 2022": "0.268511%",
                                            "Mar 2023": "0.255591%"
                                        },
                                        {
                                            "Ratio": "shareholder_funds_to_total_assets",
                                            "Mar 2021": "0.666172%",
                                            "Mar 2022": "0.634891%",
                                            "Mar 2023": "0.634914%"
                                        },
                                        {
                                            "Ratio": "cash_return_on_equity",
                                            "Mar 2021": "0.445447%",
                                            "Mar 2022": "0.444639%",
                                            "Mar 2023": "0.460112%"
                                        },
                                        {
                                            "Ratio": "asset_efficiency_ratio",
                                            "Mar 2021": "1.255569",
                                            "Mar 2022": "1.355018",
                                            "Mar 2023": "1.569484"
                                        },
                                        {
                                            "Ratio": "price_to_book_value",
                                            "Mar 2021": "13.440386",
                                            "Mar 2022": "15.308018",
                                            "Mar 2023": "12.861773"
                                        },
                                        {
                                            "Ratio": "ebit_margin",
                                            "Mar 2021": "0.258757%",
                                            "Mar 2022": "0.252636%",
                                            "Mar 2023": "0.240564%"
                                        },
                                        {
                                            "Ratio": "cash_flow_margin",
                                            "Mar 2021": "0.236342%",
                                            "Mar 2022": "0.208335%",
                                            "Mar 2023": "0.186132%"
                                        },
                                        {
                                            "Ratio": "earnings_per_share",
                                            "Mar 2021": "87.669974",
                                            "Mar 2022": "104.744336",
                                            "Mar 2023": "115.184062"
                                        },
                                        {
                                            "Ratio": "cash_ratio",
                                            "Mar 2021": "0.200791",
                                            "Mar 2022": "0.294869",
                                            "Mar 2023": "0.163529"
                                        },
                                        {
                                            "Ratio": "dividend_payout_coverage",
                                            "Mar 2021": "-3.576221",
                                            "Mar 2022": "-2.999850",
                                            "Mar 2023": "-1.014947"
                                        },
                                        {
                                            "Ratio": "net_profit_to_cash_flow_conversion",
                                            "Mar 2021": "0.835782",
                                            "Mar 2022": "0.959398",
                                            "Mar 2023": "1.004337"
                                        },
                                        {
                                            "Ratio": "other_expense_ratio",
                                            "Mar 2021": "0.173100%",
                                            "Mar 2022": "0.180403%",
                                            "Mar 2023": "0.185480%"
                                        },
                                        {
                                            "Ratio": "book_value_per_share",
                                            "Mar 2021": "235.484307",
                                            "Mar 2022": "245.541253",
                                            "Mar 2023": "249.258014"
                                        },
                                        {
                                            "Ratio": "retained_earnings_ratio",
                                            "Mar 2021": "1.334567%",
                                            "Mar 2022": "1.347457%",
                                            "Mar 2023": "1.981019%"
                                        },
                                        {
                                            "Ratio": "gross_profit_margin",
                                            "Mar 2021": "0.431857%",
                                            "Mar 2022": "0.433039%",
                                            "Mar 2023": "0.426044%"
                                        },
                                        {
                                            "Ratio": "return_on_assets",
                                            "Mar 2021": "0.248014%",
                                            "Mar 2022": "0.270835%",
                                            "Mar 2023": "0.293399%"
                                        },
                                        {
                                            "Ratio": "dividend_payout_ratio",
                                            "Mar 2021": "-0.334567%",
                                            "Mar 2022": "-0.347457%",
                                            "Mar 2023": "-0.981019%"
                                        },
                                        {
                                            "Ratio": "dividend_yield",
                                            "Mar 2021": "-0.926744%",
                                            "Mar 2022": "-0.968253%",
                                            "Mar 2023": "-3.524680%"
                                        },
                                        {
                                            "Ratio": "operating_profit_margin",
                                            "Mar 2021": "0.258757%",
                                            "Mar 2022": "0.252636%",
                                            "Mar 2023": "0.240564%"
                                        },
                                        {
                                            "Ratio": "reserves_to_equity",
                                            "Mar 2021": "223.129730",
                                            "Mar 2022": "233.898907",
                                            "Mar 2023": "236.814208"
                                        }
                                    ]
                                }
                            ],
                            [
                                {
                                    "company_name": "Reliance Jio Financial Services",
                                    "ticker": "JIOFIN",
                                    "company_number": 30,
                                    "headers": [
                                        "Ratio",
                                        "Mar 2021",
                                        "Mar 2022",
                                        "Mar 2023"
                                    ],
                                    "data": [
                                        {
                                            "Ratio": "debt_to_assets",
                                            "Mar 2021": "0.000116%",
                                            "Mar 2022": "0.000040%",
                                            "Mar 2023": "0.007043%"
                                        },
                                        {
                                            "Ratio": "receivables_turnover",
                                            "Mar 2021": "0.094619",
                                            "Mar 2022": "0.091959",
                                            "Mar 2023": "1.257970"
                                        },
                                        {
                                            "Ratio": "depreciation_to_net_profit",
                                            "Mar 2021": null,
                                            "Mar 2022": null,
                                            "Mar 2023": null
                                        },
                                        {
                                            "Ratio": "current_ratio",
                                            "Mar 2021": "29222.857143",
                                            "Mar 2022": "29601.714286",
                                            "Mar 2023": "20.942348"
                                        },
                                        {
                                            "Ratio": "capex_to_operating_cash_flow",
                                            "Mar 2021": null,
                                            "Mar 2022": null,
                                            "Mar 2023": null
                                        },
                                        {
                                            "Ratio": "operating_cash_flow_ratio",
                                            "Mar 2021": "34773.285714",
                                            "Mar 2022": "1757.714286",
                                            "Mar 2023": "2.564266"
                                        },
                                        {
                                            "Ratio": "net_working_capital",
                                            "Mar 2021": "20455.300000",
                                            "Mar 2022": "20720.500000",
                                            "Mar 2023": "159810.000000"
                                        },
                                        {
                                            "Ratio": "price_to_earnings_ratio",
                                            "Mar 2021": null,
                                            "Mar 2022": null,
                                            "Mar 2023": null
                                        },
                                        {
                                            "Ratio": "asset_turnover",
                                            "Mar 2021": "0.089607",
                                            "Mar 2022": "0.082585",
                                            "Mar 2023": "0.000450"
                                        },
                                        {
                                            "Ratio": "cash_earnings_per_share",
                                            "Mar 2021": null,
                                            "Mar 2022": null,
                                            "Mar 2023": null
                                        },
                                        {
                                            "Ratio": "cash_return_on_assets",
                                            "Mar 2021": "1.181485%",
                                            "Mar 2022": "0.055221%",
                                            "Mar 2023": "0.017880%"
                                        },
                                        {
                                            "Ratio": "return_on_investment",
                                            "Mar 2021": null,
                                            "Mar 2022": null,
                                            "Mar 2023": null
                                        },
                                        {
                                            "Ratio": "equity_ratio",
                                            "Mar 2021": "0.999884%",
                                            "Mar 2022": "0.999960%",
                                            "Mar 2023": "0.992957%"
                                        },
                                        {
                                            "Ratio": "depreciation_to_sales",
                                            "Mar 2021": null,
                                            "Mar 2022": null,
                                            "Mar 2023": null
                                        },
                                        {
                                            "Ratio": "net_income_to_operating_cf_ratio",
                                            "Mar 2021": "0.050556",
                                            "Mar 2022": "1.365735",
                                            "Mar 2023": "0.015208"
                                        },
                                        {
                                            "Ratio": "other_income_to_sales",
                                            "Mar 2021": null,
                                            "Mar 2022": "0.192381%",
                                            "Mar 2023": "0.194622%"
                                        },
                                        {
                                            "Ratio": "working_capital_to_total_assets",
                                            "Mar 2021": "0.992865%",
                                            "Mar 2022": "0.929954%",
                                            "Mar 2023": "0.139050%"
                                        },
                                        {
                                            "Ratio": "financial_leverage",
                                            "Mar 2021": "1.000117",
                                            "Mar 2022": "1.000040",
                                            "Mar 2023": "1.007093"
                                        },
                                        {
                                            "Ratio": "free_cash_flow",
                                            "Mar 2021": null,
                                            "Mar 2022": null,
                                            "Mar 2023": null
                                        },
                                        {
                                            "Ratio": "effective_tax_rate",
                                            "Mar 2021": "-0.038370%",
                                            "Mar 2022": "0.049924%",
                                            "Mar 2023": "0.366640%"
                                        },
                                        {
                                            "Ratio": "tax_burden",
                                            "Mar 2021": "-0.038370%",
                                            "Mar 2022": "0.049924%",
                                            "Mar 2023": "0.366640%"
                                        },
                                        {
                                            "Ratio": "fixed_asset_turnover",
                                            "Mar 2021": null,
                                            "Mar 2022": null,
                                            "Mar 2023": "1.306953"
                                        },
                                        {
                                            "Ratio": "debt_to_equity",
                                            "Mar 2021": null,
                                            "Mar 2022": null,
                                            "Mar 2023": null
                                        },
                                        {
                                            "Ratio": "net_profit_margin",
                                            "Mar 2021": "0.666594%",
                                            "Mar 2022": "0.913211%",
                                            "Mar 2023": "0.604566%"
                                        },
                                        {
                                            "Ratio": "return_on_equity",
                                            "Mar 2021": "0.059738%",
                                            "Mar 2022": "0.075421%",
                                            "Mar 2023": "0.000274%"
                                        },
                                        {
                                            "Ratio": "shareholder_funds_to_total_assets",
                                            "Mar 2021": "0.999884%",
                                            "Mar 2022": "0.999960%",
                                            "Mar 2023": "0.992957%"
                                        },
                                        {
                                            "Ratio": "cash_return_on_equity",
                                            "Mar 2021": "1.181622%",
                                            "Mar 2022": "0.055224%",
                                            "Mar 2023": "0.018006%"
                                        },
                                        {
                                            "Ratio": "asset_efficiency_ratio",
                                            "Mar 2021": "0.089607",
                                            "Mar 2022": "0.082585",
                                            "Mar 2023": "0.000450"
                                        },
                                        {
                                            "Ratio": "price_to_book_value",
                                            "Mar 2021": null,
                                            "Mar 2022": null,
                                            "Mar 2023": null
                                        },
                                        {
                                            "Ratio": "ebit_margin",
                                            "Mar 2021": "0.664428%",
                                            "Mar 2022": "0.961741%",
                                            "Mar 2023": "0.892436%"
                                        },
                                        {
                                            "Ratio": "cash_flow_margin",
                                            "Mar 2021": "13.185255%",
                                            "Mar 2022": "0.668659%",
                                            "Mar 2023": "39.754305%"
                                        },
                                        {
                                            "Ratio": "cash_flow_to_debt",
                                            "Mar 2021": null,
                                            "Mar 2022": null,
                                            "Mar 2023": null
                                        },
                                        {
                                            "Ratio": "earnings_per_share",
                                            "Mar 2021": "615.300000",
                                            "Mar 2022": "840.200000",
                                            "Mar 2023": "156.250000"
                                        },
                                        {
                                            "Ratio": "cash_ratio",
                                            "Mar 2021": "5.142857",
                                            "Mar 2022": "7.285714",
                                            "Mar 2023": "0.078766"
                                        },
                                        {
                                            "Ratio": "net_profit_to_cash_flow_conversion",
                                            "Mar 2021": "0.050556",
                                            "Mar 2022": "1.365735",
                                            "Mar 2023": "0.015208"
                                        },
                                        {
                                            "Ratio": "other_expense_ratio",
                                            "Mar 2021": "0.335572%",
                                            "Mar 2022": "0.038259%",
                                            "Mar 2023": "0.107564%"
                                        },
                                        {
                                            "Ratio": "book_value_per_share",
                                            "Mar 2021": "10299.950000",
                                            "Mar 2022": "11140.150000",
                                            "Mar 2023": "570601.700000"
                                        },
                                        {
                                            "Ratio": "gross_profit_margin",
                                            "Mar 2021": "1.000000%",
                                            "Mar 2022": "1.000000%",
                                            "Mar 2023": "1.000000%"
                                        },
                                        {
                                            "Ratio": "return_on_assets",
                                            "Mar 2021": "0.059731%",
                                            "Mar 2022": "0.075418%",
                                            "Mar 2023": "0.000272%"
                                        },
                                        {
                                            "Ratio": "operating_profit_margin",
                                            "Mar 2021": "0.664428%",
                                            "Mar 2022": "0.961741%",
                                            "Mar 2023": "0.892436%"
                                        },
                                        {
                                            "Ratio": "reserves_to_equity",
                                            "Mar 2021": "581.811881",
                                            "Mar 2022": "665.000000",
                                            "Mar 2023": "5189.326733"
                                        }
                                    ]
                                }
                            ]
                        ],
                        "display_recommendations": {
                            "chart": true,
                            "company_overview": false,
                            "llm_response": true,
                            "table": true
                        },
                        "classification": {
                            "chart_endpoint_type": "ratios",
                            "chart_parameters": [
                                "net_profit_margin",
                                "gross_profit_margin",
                                "return_on_equity",
                                "return_on_assets",
                                "current_ratio",
                                "debt_to_equity",
                                "asset_turnover",
                                "ebitda_margin"
                            ],
                            "confidence": 0.95,
                            "data_source_type": "hybrid",
                            "display_components": {
                                "chart": true,
                                "company_overview": false,
                                "llm_response": true,
                                "table": true
                            },
                            "endpoint_mode": "base",
                            "endpoint_type": "ratios",
                            "financial_metrics": [],
                            "identified_parameters": {
                                "balance_sheet": [
                                    "total assets",
                                    "total equity",
                                    "cash and equivalents",
                                    "total liabilities"
                                ],
                                "cashflow": [
                                    "cash from operations",
                                    "free cash flow",
                                    "capital expenditure"
                                ],
                                "financial_ratios": [
                                    "net_profit_margin",
                                    "gross_profit_margin",
                                    "return_on_equity",
                                    "return_on_assets",
                                    "current_ratio",
                                    "debt_to_equity",
                                    "asset_turnover",
                                    "ebitda_margin"
                                ],
                                "profit_and_loss": [
                                    "net income",
                                    "total revenues",
                                    "operating income",
                                    "gross profit",
                                    "ebitda"
                                ],
                                "shareholder": []
                            },
                            "parameter_filters": {
                                "balance_sheet": [
                                    "total assets",
                                    "total equity",
                                    "cash and equivalents",
                                    "total liabilities"
                                ],
                                "balance_sheet_related": [
                                    "short term investments",
                                    "total current assets",
                                    "preferred stock redeemable"
                                ],
                                "cashflow": [
                                    "cash from operations",
                                    "free cash flow",
                                    "capital expenditure"
                                ],
                                "cashflow_related": [
                                    "sale of property, plant, and equipment",
                                    "net income",
                                    "(gain) loss on sale of asset"
                                ],
                                "financial_ratios": [
                                    "net_profit_margin",
                                    "gross_profit_margin",
                                    "return_on_equity",
                                    "return_on_assets",
                                    "current_ratio",
                                    "debt_to_equity",
                                    "asset_turnover",
                                    "ebitda_margin"
                                ],
                                "financial_ratios_related": [
                                    "operating_profit_margin",
                                    "gross_profit_margin",
                                    "equity_ratio"
                                ],
                                "profit_and_loss": [
                                    "net income",
                                    "total revenues",
                                    "operating income",
                                    "gross profit",
                                    "ebitda"
                                ],
                                "profit_and_loss_related": [
                                    "operating income",
                                    "gross profit",
                                    "net income"
                                ]
                            },
                            "query_type": "ratio_analysis",
                            "reasoning": "Identified SQL tables: balance_sheet, profit_and_loss, cashflow, financial_ratios; Identified 20 specific parameters; Vector stores needed: earnings",
                            "required_sql_tables": [
                                "balance_sheet",
                                "profit_and_loss",
                                "cashflow",
                                "financial_ratios"
                            ],
                            "vector_stores_needed": [
                                "earnings"
                            ]
                        },
                        "context_info": {
                            "endpoint_routing": {
                                "endpoint_type": "ratios",
                                "endpoint_mode": "base",
                                "chart_endpoint_type": "ratios"
                            },
                            "parameter_filtering": {
                                "total_parameters": 20,
                                "filtered_tables": [
                                    "balance_sheet",
                                    "cashflow",
                                    "financial_ratios",
                                    "profit_and_loss",
                                    "shareholder"
                                ],
                                "chart_parameters": [
                                    "net_profit_margin",
                                    "gross_profit_margin",
                                    "return_on_equity",
                                    "return_on_assets",
                                    "current_ratio",
                                    "debt_to_equity",
                                    "asset_turnover",
                                    "ebitda_margin"
                                ]
                            },
                            "data_availability": {
                                "company_overviews": 0,
                                "shareholding_patterns": 0,
                                "charts": 1,
                                "financial_tables": 2
                            },
                            "resolved_companies": [
                                {
                                    "aliases": [
                                        "tata consultancy services",
                                        "tcs"
                                    ],
                                    "full_name": "Tata Consultancy Services Limited",
                                    "sector": "Information Technology",
                                    "ticker": "TCS"
                                },
                                {
                                    "aliases": [
                                        "jio financial services",
                                        "jio financial",
                                        "jiofin"
                                    ],
                                    "full_name": "Jio Financial Services Limited",
                                    "sector": "Financial Services",
                                    "ticker": "JIOFIN"
                                }
                            ],
                            "total_context_length": 115682
                        }
                    },
                    timestamp: "2023-10-01T12:00:00Z"
                }
            ]
        }
    },
    currentLLMResponse: {}
}

export const reducer = (state = initState, { type, payload }) => {
    switch (type) {
        case LOADING:
            return {
                ...state,
                isLoading: true,
                isError: false
            };
        case ERROR:
            return {
                ...state,
                isLoading: false,
                isError: true
            };
        case CREATE_CHAT_HISTORY:
            state.chatHistory.push(payload.chatId);
            state.chatHistoryMap[payload.chatId] = {
                title: payload.title,
                history: [payload.historyData]
            }
            return {
                ...state,
                isLoading: false,
                isError: false,
            };
        case SET_CHAT_HISTORY:
            state.chatHistoryMap[payload.chatId].history.push(payload.historyData);
            return {
                ...state,
                isLoading: false,
                isError: false
            };
        case REMOVE_CHAT_HISTORY:
            return {
                ...state,
                isLoading: false,
                isError: false,
                chatHistory: state.chatHistory.filter(chatId => chatId !== payload.chatId),
                chatHistoryMap: Object.keys(state.chatHistoryMap)
                    .filter(chatId => chatId !== payload.chatId)
                    .reduce((acc, chatId) => {
                        acc[chatId] = state.chatHistoryMap[chatId];
                        return acc;
                    }, {})
            };
        case SET_CURRENT_LLM_RESPONSE:
            return {
                ...state,
                isLoading: false,
                isError: false,
                currentLLMResponse: {
                    ...state.currentLLMResponse,
                    [payload.chatId]: payload.response
                }
            };
        default:
            return state;
    }
}