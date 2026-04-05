import { SearchCompanyInput } from "../../Components/SearchCompanyInput";
import "../../Styles/Pages/Main-Pages/Company.css";
import { Outlet, useLocation } from "react-router-dom";

export const Company = () => {
    const location = useLocation();
    const isCompanyChild = location.pathname.startsWith("/company/");

    if (isCompanyChild) {
        return <Outlet />;
    }
    return (
        <div className="company">
            <h1>Search for Company</h1>
            <SearchCompanyInput />
        </div>
    )
};