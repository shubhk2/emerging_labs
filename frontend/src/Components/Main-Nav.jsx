import { NavLink } from 'react-router-dom';
import '../Styles/Components/Main-Nav.css';
import { SearchCompanyInput } from './SearchCompanyInput'; // Adjust the path as needed
import { useSelector } from 'react-redux';

export const MainNav = () => {
    const { selectedCompany } = useSelector(store => store.mainReducer);
    const { companyId } = selectedCompany;
    return (
        <div className="main-nav">
            <NavLink tabIndex={-1} to={`/company/${companyId}/overview`}><button className='button-secondary'>Overview</button></NavLink>
            <NavLink tabIndex={-1} to={`/company/${companyId}/financial`}><button className='button-secondary'>Financial</button></NavLink>
            <SearchCompanyInput />
            <NavLink tabIndex={-1} to={`/company/${companyId}/forecasting`}><button className='button-secondary'>Forecasting</button></NavLink>
            <NavLink tabIndex={-1} to={`/company/${companyId}/investor-info`}><button className='button-secondary'>Investor Info</button></NavLink>
        </div>
    );
};