import ReactMarkdown from 'react-markdown';
import { PlotlyGraph } from "../PlotlyGraph";
import {useSelector} from "react-redux";
import {Overview} from "../../Pages/Main-Pages/Company/Overview";
import {splitOverview} from "../../Utils/utilities";

export const ChatMessageScreen = ({ chatId }) => {
    const { history } = useSelector(state => state?.chatReducer?.chatHistoryMap[chatId] || []);

    const renderLLMResponse = (llmResponse, textOverview, ratiosData, financialData, shareholdingData, overviewData, compID, dividendData, insiderData, rptData, pledgedData, corporateData) => {
    const responseArray = Array.isArray(llmResponse) ? llmResponse : ["Server is currently unavailable"];
    return responseArray.map((segment, index) => {
        if (typeof segment === 'string') {
            if (overviewData && index === 0 && textOverview) {
                return (
                    <>
                        <div key={`overview-${index}`} className="company-overview">
                            <h2>Company Overview</h2>
                            <ReactMarkdown>{splitOverview(overviewData).overview}</ReactMarkdown>
                        </div>
                        <ReactMarkdown key={index}>{segment}</ReactMarkdown>
                    </>
                );
            } else {
                return <ReactMarkdown key={index}>{segment}</ReactMarkdown>;
            }
        } else if (segment.placeholder === '~COMPREHENSIVE_RATIOS_TABLE~' || segment.placeholder === '~RATIOS_TABLE~') {
            return (
                <div key={index} className="company-stats-table">
                    <h3>Comprehensive Ratios Table</h3>
                    <table>
                        <thead>
                            <tr>
                                {ratiosData?.headers?.map((header, i) => (
                                    <th key={i}>{header}</th>
                                ))}
                            </tr>
                        </thead>
                        <tbody>
                            {ratiosData?.data?.map((row, i) => (
                                <tr key={i}>
                                    {ratiosData?.headers?.map((header, j) => (
                                        <td key={j}>{row[header] || ''}</td>
                                    ))}
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            );
        } else if (segment.placeholder === '~FINANCIAL_DATA_TABLE~' || segment.placeholder === '~FINANCIAL_PARAMETERS_TABLE~') {
            return (
                <div key={index} className="company-stats-table">
                    <h3>Financial Table</h3>
                    <table>
                        <thead>
                            <tr>
                                {financialData?.headers?.map((header, i) => (
                                    <th key={i}>{header}</th>
                                ))}
                            </tr>
                        </thead>
                        <tbody>
                            {financialData?.data?.map((row, i) => (
                                <tr key={i}>
                                    {financialData?.headers?.map((header, j) => (
                                        <td key={j}>{row[header] || ''}</td>
                                    ))}
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            );
        } else if (segment.placeholder === '~SHAREHOLDING_TABLE~') {
            return (
                <div key={index} className="shareholding-data-table">
                    <h3>Shareholding Table</h3>
                    <table>
                        <thead>
                            <tr>
                                {shareholdingData?.headers?.map((header, i) => (
                                    <th key={i}>{header}</th>
                                ))}
                            </tr>
                        </thead>
                        <tbody>
                            {shareholdingData?.data?.map((row, i) => (
                                <tr key={i}>
                                    {shareholdingData?.headers?.map((header, j) => (
                                        <td key={j}>{row[header] || ''}</td>
                                    ))}
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            );
        } else if (segment.placeholder === '~OVERVIEW_STATS_TABLE~') {
            return (
                <Overview ID={compID} />
            );
        } else if (segment.placeholder === '~DIVIDEND_TABLE~') {
            return (
                <div key={index} className="dividend-data-table">
                    <h3>Dividend Table</h3>
                    <table>
                        <thead>
                            <tr>
                                {dividendData?.headers?.map((header, i) => (
                                    <th key={i}>{header}</th>
                                ))}
                            </tr>
                        </thead>
                        <tbody>
                            {dividendData?.data?.map((row, i) => (
                                <tr key={i}>
                                    {dividendData?.headers?.map((header, j) => (
                                        <td key={j}>{row[header] || ''}</td>
                                    ))}
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            );
        } else if (segment.placeholder === '~RPT_TABLE~') {
            return (
                <div key={index} className="rpt-data-table">
                    <h3>Rpt Table</h3>
                    <table>
                        <thead>
                            <tr>
                                {rptData?.headers?.map((header, i) => (
                                    <th key={i}>{header}</th>
                                ))}
                            </tr>
                        </thead>
                        <tbody>
                            {rptData?.data?.map((row, i) => (
                                <tr key={i}>
                                    {rptData?.headers?.map((header, j) => (
                                        <td key={j}>{row[header] || ''}</td>
                                    ))}
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            );
        } else if (segment.placeholder === '~PLEDGED_DATA_TABLE~') {
            return (
                <div key={index} className="pledged-data-table">
                    <h3>Pledged Data Table</h3>
                    <table>
                        <thead>
                            <tr>
                                {pledgedData?.headers?.map((header, i) => (
                                    <th key={i}>{header}</th>
                                ))}
                            </tr>
                        </thead>
                        <tbody>
                            {pledgedData?.data?.map((row, i) => (
                                <tr key={i}>
                                    {pledgedData?.headers?.map((header, j) => (
                                        <td key={j}>{row[header] || ''}</td>
                                    ))}
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            );
        } else if (segment.placeholder === '~CORPORATE_GOVERNANCE_TABLE~') {
            return (
                <div key={index} className="corporate-data-table">
                    <h3>Corporate Governance Table</h3>
                    <table>
                        <thead>
                            <tr>
                                {corporateData?.headers?.map((header, i) => (
                                    <th key={i}>{header}</th>
                                ))}
                            </tr>
                        </thead>
                        <tbody>
                            {corporateData?.data?.map((row, i) => (
                                <tr key={i}>
                                    {corporateData?.headers?.map((header, j) => (
                                        <td key={j}>{row[header] || ''}</td>
                                    ))}
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            );
        } else if (segment.placeholder === '~INSIDER_TRADING_TABLE~') {
            return (
                <div key={index} className="insider-data-table">
                    <h3>Insider Trading Table</h3>
                    <table>
                        <thead>
                            <tr>
                                {insiderData?.headers?.map((header, i) => (
                                    <th key={i}>{header}</th>
                                ))}
                            </tr>
                        </thead>
                        <tbody>
                            {insiderData?.data?.map((row, i) => (
                                <tr key={i}>
                                    {insiderData?.headers?.map((header, j) => (
                                        <td key={j}>{row[header] || ''}</td>
                                    ))}
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            );
        }   
        return null;
    });
};

    return (
        <>
            {history && history.length > 0 &&
                history.map(({ query, response, timestamp }, i) => {
                    const graph = (response?.chart_data?.plotly_json && JSON.parse(response?.chart_data?.plotly_json)) || {};
                    const llmResponse = response?.llm_response || [];
                    const ratiosData = response?.consolidated_data?.ratios?.[0] || {};
                    const financialsData = response?.consolidated_data?.financial_statements?.balance_sheet?.[0] || {};
                    const shareholdingData = response?.consolidated_data?.shareholding_pattern?.[0] || {};
                    const dividendData = response?.consolidated_data?.dividend?.[0] || {};
                    const insiderData = response?.consolidated_data?.insider_trading?.[0] || {};
                    const rptData = response?.consolidated_data?.rpt?.[0] || {};
                    const pledgedData = response?.consolidated_data?.pledged_data?.[0] || {};
                    const corporateData = response?.consolidated_data?.corporate_governance?.[0] || {};
                    const overviewData = response?.company_overviews?.[0]?.overview?.overview_text || "no overview text available";
                    const textOverview = response?.enhanced_context_data?.classification?.display_components?.company_overview || false;
                    const compID = response?.company_ids?.[0]?.toString() || "";
                    console.log(llmResponse);

                    if(llmResponse === "Loading response...") {
                        return;
                    }

                    return (
                        <div className="query-response-container" key={i}>
                            <div className="chat-timestamp">
                                {new Date(timestamp).toLocaleString()}
                            </div>
                            <div className="chat-query">
                                {query}
                            </div>
                            <div className="chat-response">
                                {renderLLMResponse(llmResponse, textOverview, ratiosData, financialsData, shareholdingData, overviewData, compID, dividendData, insiderData, rptData, pledgedData, corporateData)}
                                {Object.keys(graph).length > 0 && (
                                    <div className="chart-plot">
                                        <PlotlyGraph graphData={graph} />
                                    </div>
                                )}
                            </div>
                        </div>
                    );
                })
            }
        </>
    );
};
