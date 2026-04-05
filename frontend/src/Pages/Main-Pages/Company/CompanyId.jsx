import { useEffect } from "react";
import { useDispatch, useSelector } from "react-redux";
import { Outlet, useLocation, useNavigate, useParams } from "react-router-dom";
import { createTab, getCompanyDetailsById, setActiveTab } from "../../../Redux/MainReducer/action";

export const CompanyId = () => {
    const { compId } = useParams();
    const dispatch = useDispatch();
    const navigate = useNavigate();
    const location = useLocation();
    const { userTabs } = useSelector(store => store.mainReducer);
    useEffect(() => {
        if (isNaN(+compId)) {
            navigate("/company");
        }
        const isBasePath = location.pathname === `/company/${compId}`;
        if (!userTabs[compId]) {
            dispatch(getCompanyDetailsById(compId))
                .then(res => {
                    const { id, full_name, ticker } = res;
                    if (!id || !full_name) {
                        return navigate("/company");
                    }
                    const tabPayload = {
                        tabId: `tab-${compId}`,
                        companyName: full_name,
                        companyId: id,
                        companyTicker: ticker
                    }
                    dispatch(createTab(tabPayload));
                    dispatch(setActiveTab(tabPayload));
                    isBasePath && navigate(`/company/${compId}/overview`);
                });
        } else {
            dispatch(setActiveTab(userTabs[compId]));
            isBasePath && navigate(`/company/${compId}/overview`);
        }
    }, [compId, dispatch, navigate, userTabs, location.pathname]);
    if (isNaN(+compId)) return null;

    return <Outlet />
}