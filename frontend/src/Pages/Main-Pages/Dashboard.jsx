
import { AssetAllocationPieChart } from '../../Components/Dashboard/AssetAllocationPieChart';
import { EconomicIndicatorPanel } from '../../Components/Dashboard/EconomicIndicatorPanel';
import { HoldingsTable } from '../../Components/Dashboard/HoldingsTable';
import { NewsSwiper } from '../../Components/Dashboard/NewsSwiper';
import { PortfolioTimelineChart } from '../../Components/Dashboard/PortfolioTimelineChart';
import { WatchlistSwiper } from '../../Components/Dashboard/WatchlistSwiper';
import '../../Styles/Pages/Main-Pages/Dashboard.css';

export const Dashboard = () => {
    return (
        <div className='dashboard'>
            <NewsSwiper />
            <WatchlistSwiper />
            <div className='asset-portfolio'>
                <AssetAllocationPieChart />
                <PortfolioTimelineChart />
                <EconomicIndicatorPanel />
            </div>
            <HoldingsTable />
        </div>
    )
}