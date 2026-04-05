import { useLocation } from 'react-router-dom';
import { HomeNav } from '../Components/Home-Nav';
import { MainNav } from '../Components/Main-Nav';
import { TabFoot } from '../Components/Tab-Foot';

export const Layout = ({ children }) => {
    const location = useLocation();

    const companyTabs = ['overview', 'financial', 'forecasting', 'investor-info'];

    const pathSegments = location.pathname.split('/').filter(Boolean);

    // Get the second-to-last segment (after companyId) to match company tab pages
    const tabSegment = pathSegments[2]; // /company/:compId/:tab

    const isCompanyPath = companyTabs.includes(tabSegment);
    return (
        <>
            <HomeNav />
            {isCompanyPath && <MainNav />}
            {children}
            {(isCompanyPath || location.pathname === "/company") && <TabFoot />}
        </>
    );
};