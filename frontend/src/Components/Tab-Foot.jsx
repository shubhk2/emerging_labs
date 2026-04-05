import { useDispatch, useSelector } from "react-redux";
import "../Styles/Components/Tab-Foot.css";
import { NavLink } from "react-router-dom";
import { IoCloseCircle } from "react-icons/io5";
import { removeTab } from "../Redux/MainReducer/action";

export const TabFoot = () => {
    const dispatch = useDispatch();
    const { userTabs, selectedCompany } = useSelector(store => store.mainReducer);
    const tabCompanyIds = Object.keys(userTabs);
    const handleTabRemoval = (e, payload) => {
        e.preventDefault();
        e.stopPropagation();
        dispatch(removeTab(payload));
    }
    return (
        <>
            <footer className="tabs-footer">
                {
                    tabCompanyIds.length > 0 && tabCompanyIds.map(companyId => {
                        const tab = userTabs[companyId];
                        return (
                            <NavLink to={`/company/${tab.companyId}/overview`} key={tab.tabId} className={`tab ${selectedCompany.tabId === tab.tabId && 'active'}`} >
                                <p>{tab.companyName}</p>
                                <span onClick={e => handleTabRemoval(e, tab)} className="remove-tab"><IoCloseCircle /></span>
                            </NavLink>
                        )
                    })
                }
            </footer>
        </>
    )
}