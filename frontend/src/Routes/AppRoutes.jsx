import { Navigate, Route, Routes } from 'react-router-dom';
import { Home } from '../Pages/Landing-Pages/Home';
import { Signout } from '../Pages/Landing-Pages/Signout';
import { Dashboard } from '../Pages/Main-Pages/Dashboard';
import { Layout } from './Layout';
import { Overview } from '../Pages/Main-Pages/Company/Overview';
import { Financial } from '../Pages/Main-Pages/Company/Financial';
import { RatioAnalysis } from '../Pages/Main-Pages/RatioAnalysis';
import { ValuationModeling } from '../Pages/Main-Pages/ValuationModeling';
import { PeerComparison } from '../Pages/Main-Pages/PeerComparison';
import { Forecasting } from '../Pages/Main-Pages/Company/Forecasting';
import { ReportsDownloads } from '../Pages/Main-Pages/ReportsDownloads';
import { InvestorInfo } from '../Pages/Main-Pages/Company/InvestorInfo';
import { Company } from '../Pages/Main-Pages/Company';
import { CompanyId } from '../Pages/Main-Pages/Company/CompanyId';
import { Account } from '../Pages/Main-Pages/Account';
import { NotFound } from '../Pages/Main-Pages/NotFound';
import { SigninSignup } from '../Pages/Landing-Pages/SigninSignup';

export const AppRoutes = () => {
    return (
        <Layout>
            <Routes>
                <Route path="/" element={<Home />} />
                <Route path="/signin" element={<SigninSignup />} />
                <Route path="/signup" element={<SigninSignup />} />
                <Route path="/signout" element={<Signout />} />
                <Route path="/dashboard" element={<Dashboard />} />
                <Route path="/account" element={<Account />} />
                <Route path="/company" element={<Company />} >
                    <Route path=":compId" element={<CompanyId />} >
                        <Route path="overview" element={<Overview />} />
                        <Route path="financial" element={<Financial />} />
                        <Route path="financial/:type" element={<Financial />} />
                        <Route path="forecasting" element={<Forecasting />} />
                        <Route path="investor-info" element={<InvestorInfo />} />
                        <Route path="investor-info/:type" element={<InvestorInfo />} />
                        <Route path="*" element={<Navigate to={"/company"} replace />} />
                    </Route>
                </Route>
                <Route path="/ratio-analysis" element={<RatioAnalysis />} />
                <Route path="/valuation-modeling" element={<ValuationModeling />} />
                <Route path="/peer-comparison" element={<PeerComparison />} />
                <Route path="/reports-downloads" element={<ReportsDownloads />} />
                <Route path="*" element={<NotFound />} />
            </Routes>
        </Layout>
    );
};
