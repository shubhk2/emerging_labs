import '../../../Styles/Pages/Main-Pages/Company/Overview.css';
import ReactMarkdown from 'react-markdown';
import { useEffect, useState } from 'react';
import { useDispatch, useSelector } from 'react-redux';
import { getOverviewDataFunc, getOverviewGraphDataFunc } from '../../../Redux/MainReducer/action';
import { useParams } from 'react-router-dom';
import { splitOverview } from '../../../Utils/utilities';
import { PlotlyGraph } from '../../../Components/PlotlyGraph';

export const Overview = ({ID=""}) => {
    let { compId } = useParams() || {};
    if (ID !== "") {
        compId = ID;
    }
    const [fullOverview, setFullOverview] = useState(false);
    const { overview: { data: { overview, stats }, graph } } = useSelector(store => store.mainReducer);
    const dispatch = useDispatch();
    const [graphPeriod, setGraphPeriod] = useState('10yr');
    const [graphType, setGraphType] = useState('price');
    const processStatsData = (key, statCategory) => {
        if (!stats[key]) {
            return;
        }
        const { columns, values } = stats[key];
        if (!values || !columns) {
            return;
        }
        return (
            <>
                <h3 className='stat-category'>{statCategory}</h3>
                {
                    values.map(value => {
                        return (
                            <div className='stat-item' key={value}>
                                <span className='item-name'>{value[0]}</span>
                                <span className='item-value'>{value[1]} {value[2]}</span>
                            </div>
                        )
                    })
                }
            </>
        )
    }
    useEffect(() => {
        dispatch(getOverviewDataFunc(compId))
    }, [compId, dispatch])
    useEffect(() => {
        overview && stats && dispatch(getOverviewGraphDataFunc(compId, graphType, graphPeriod));
    }, [graphPeriod, graphType, compId, overview, stats, dispatch]);
    if (ID !== "") {
        return(
            <div className='overview-container'>
                <div className='statistics'>
                    <h2 className='stats-heading'>Company Statistics</h2>
                    <div className="profile">{stats && processStatsData("profile_metrics", 'Profile')}</div>
                    <div className="margins">{stats && processStatsData("margins_metrics", 'Margins')}</div>
                    <div
                        className="returns-5yr">{stats && processStatsData("returns_5yr_avg_metrics", 'Returns (5Yr Avg)')}</div>
                    <div
                        className="valuation-ttm">{stats && processStatsData("valuation_ttm_metrics", 'Valuation (TTM)')}</div>
                    <div
                        className="valuation-ntm">{stats && processStatsData("valuation_ntm_metrics", 'Valuation (NTM)')}</div>
                    <div
                        className="financial-health">{stats && processStatsData("financial_health_metrics", 'Financial Health')}</div>
                    <div className="growth">{stats && processStatsData("growth_cagr_metrics", 'Growth (CAGR)')}</div>
                </div>
            </div>
        )
    } else {
        return (
            <div className='overview-container'>
                <div className='overview'>
                    <h2 className='overview-heading'>Company Overview</h2>
                    <ReactMarkdown>{splitOverview(overview?.overview_text, fullOverview)?.overview}</ReactMarkdown>
                    <button className='overview-show-more'
                            onClick={() => setFullOverview(!fullOverview)}>{fullOverview ? 'Show Less' : 'Show More'}</button>
                </div>
                <div className='chart'>
                    <div className='chart-controls'>
                        <div className='chart-periods'>
                            <button
                                className={`chart-period-button button-secondary ${graphPeriod === '1month' && 'active'}`}
                                onClick={() => setGraphPeriod('1month')}>1 Mon
                            </button>
                            <button
                                className={`chart-period-button button-secondary ${graphPeriod === '6month' && 'active'}`}
                                onClick={() => setGraphPeriod('6month')}>6 Mon
                            </button>
                            <button
                                className={`chart-period-button button-secondary ${graphPeriod === '1yr' && 'active'}`}
                                onClick={() => setGraphPeriod('1yr')}>1 Yr
                            </button>
                            <button
                                className={`chart-period-button button-secondary ${graphPeriod === '3yr' && 'active'}`}
                                onClick={() => setGraphPeriod('3yr')}>3 Yr
                            </button>
                            <button
                                className={`chart-period-button button-secondary ${graphPeriod === '5yr' && 'active'}`}
                                onClick={() => setGraphPeriod('5yr')}>5 Yr
                            </button>
                            <button
                                className={`chart-period-button button-secondary ${graphPeriod === '10yr' && 'active'}`}
                                onClick={() => setGraphPeriod('10yr')}>10 Yr
                            </button>
                        </div>
                        <select className='chart-type-select' onChange={(e) => setGraphType(e.target.value)}
                                value={graphType}>
                            <option value={'price'}>Price</option>
                            <option value={'dma50'} title='50-Day Moving Average'>DMA50</option>
                            <option value={'dma200'} title='100-Day Moving Average'>DMA200</option>
                        </select>
                        <PlotlyGraph graphData={graph}/>
                    </div>
                </div>
                <div className='statistics'>
                    <h2 className='stats-heading'>Company Statistics</h2>
                    <div className="profile">{stats && processStatsData("profile_metrics", 'Profile')}</div>
                    <div className="margins">{stats && processStatsData("margins_metrics", 'Margins')}</div>
                    <div
                        className="returns-5yr">{stats && processStatsData("returns_5yr_avg_metrics", 'Returns (5Yr Avg)')}</div>
                    <div
                        className="valuation-ttm">{stats && processStatsData("valuation_ttm_metrics", 'Valuation (TTM)')}</div>
                    <div
                        className="valuation-ntm">{stats && processStatsData("valuation_ntm_metrics", 'Valuation (NTM)')}</div>
                    <div
                        className="financial-health">{stats && processStatsData("financial_health_metrics", 'Financial Health')}</div>
                    <div className="growth">{stats && processStatsData("growth_cagr_metrics", 'Growth (CAGR)')}</div>
                </div>
            </div>
        )
    }
}
