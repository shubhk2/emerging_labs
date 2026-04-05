import axios from "axios";
import { CREATE_TAB, ERROR, GET_FINANCIAL_DATA, GET_INVESTOR_INFO_DATA, GET_OVERVIEW_DATA, GET_OVERVIEW_GRAPH_DATA, LOADING, REMOVE_ERROR, REMOVE_TAB, RESET_SEARCH_RESULTS, SEARCH_COMPANY, SET_ACTIVE_TAB, STOP_LOADING } from "./actionTypes";

export const loading = () => {
    return {
        type: LOADING,
    };
}
export const stopLoading = () => {
    return {
        type: STOP_LOADING,
    }
}
export const error = () => {
    return {
        type: ERROR,
    }
}
export const removeError = () => {
    return {
        type: REMOVE_ERROR,
    }
}
export const searchCompany = (payload) => {
    return {
        type: SEARCH_COMPANY,
        payload
    }
}
export const resetSearchResults = () => {
    return {
        type: RESET_SEARCH_RESULTS
    }
}
export const getOverviewData = (payload) => {
    return {
        type: GET_OVERVIEW_DATA,
        payload
    }
}
export const getOverviewGraphData = (payload) => {
    return {
        type: GET_OVERVIEW_GRAPH_DATA,
        payload
    }
}
export const getFinancialData = (payload) => {
    return {
        type: GET_FINANCIAL_DATA,
        payload
    }
}
export const getInvertorInfoData = (payload) => {
    return {
        type: GET_INVESTOR_INFO_DATA,
        payload
    }
}
export const createTab = (payload) => {
    return {
        type: CREATE_TAB,
        payload
    }
}
export const removeTab = (payload) => {
    return {
        type: REMOVE_TAB,
        payload
    }
}
export const setActiveTab = (payload) => {
    return {
        type: SET_ACTIVE_TAB,
        payload
    }
}

export const searchCompanyFunc = (searchString, limit = 10) => dispatch => {
    dispatch(loading());
    const request = {
        method: "get",
        url: `${process.env.REACT_APP_BACKEND_URL || process.env.REACT_APP_BACKEND_URL_LOCAL}/search/companies?q=${searchString}&limit=${limit}`,
        headers: {
            "X-API-Key": process.env.REACT_APP_CODE
        }
    }
    return axios(request)
        .then((res) => {
            dispatch(searchCompany(res.data));
        })
        .catch((err) => {
            console.error(err?.response?.data || err);
            dispatch(error());
        });
}
export const getCompanyDetailsById = id => dispatch => {
    dispatch(loading());
    const request = {
        method: "get",
        url: `${process.env.REACT_APP_BACKEND_URL || process.env.REACT_APP_BACKEND_URL_LOCAL}/search/company/${id}`,
        headers: {
            "X-API-Key": process.env.REACT_APP_CODE
        }
    }
    return axios(request)
        .then(res => {
            return res?.data?.result || {};
        })
        .catch(err => {
            console.error(err?.response?.data || err);
            dispatch(error());
            return {};
        })
}
export const getOverviewDataFunc = id => dispatch => {
    dispatch(loading());
    const request = {
        method: "get",
        url: `${process.env.REACT_APP_BACKEND_URL || process.env.REACT_APP_BACKEND_URL_LOCAL}/overview/company/${id}`,
        headers: {
            "X-API-Key": process.env.REACT_APP_CODE
        }
    }
    return axios(request)
        .then((res) => {
            dispatch(getOverviewData({ data: res.data }));
        })
        .catch((err) => {
            dispatch(error());
            throw new Error(err?.response?.data || err);
        });
}
export const getOverviewGraphDataFunc = (id, type = "price", period = "10yr") => dispatch => {
    dispatch(loading());
    const request = {
        method: "get",
        url: `${process.env.REACT_APP_BACKEND_URL || process.env.REACT_APP_BACKEND_URL_LOCAL}/stock_data/${type}/${period}/chart?company_number=${id}`,
        headers: {
            "X-API-Key": process.env.REACT_APP_CODE
        }
    }
    return axios(request)
        .then((res) => {
            dispatch(getOverviewGraphData({ data: res.data }));
        })
        .catch((err) => {
            dispatch(error());
            throw new Error(err?.response?.data || err);
        });
}
export const getFinancialDataFunc = (id, type, start = '', end = '') => dispatch => {
    dispatch(loading());
    const request = {
        method: "get",
        url: `${process.env.REACT_APP_BACKEND_URL || process.env.REACT_APP_BACKEND_URL_LOCAL}/financials?company_number=${id}&statement_type=${type}`,
        headers: {
            "X-API-Key": process.env.REACT_APP_CODE
        }
    }
    if (type === 'ratio') {
        request.url = `${process.env.REACT_APP_BACKEND_URL || process.env.REACT_APP_BACKEND_URL_LOCAL}/ratios?company_numbers=${id}`
    }
    if (start) request.url += `&start_year=${start}`;
    if (end) request.url += `&end_year=${end}`;
    return axios(request)
        .then(res => {
            if (type === 'ratio') {
                dispatch(getFinancialData({ type, data: res.data[0] }));
                return;
            }
            dispatch(getFinancialData({ type, data: res.data }));
        })
        .catch(err => {
            console.error(err?.response?.data || err);
            dispatch(error());
        })
}
export const getInvertorInfoDataFunc = (id, type, quarter = 1) => dispatch => {
    dispatch(loading());
    const config = {
        method: 'get',
        url: `${process.env.REACT_APP_BACKEND_URL}/${type}?company_number=${id}`,
        headers: {
            "X-API-Key": process.env.REACT_APP_CODE
        }
    }

    if (type === "earning_calls") {
        config.url = `${process.env.REACT_APP_BACKEND_URL}/earning_calls/earning_calls_files?company_number=${id}&quarter=${quarter}`
    } else if (type === "quarterly") {
        config.url = `${process.env.REACT_APP_BACKEND_URL}/quarterly_files/all?company_number=${id}`
    } else if (type === "annual") {
        config.url = `${process.env.REACT_APP_BACKEND_URL}/annual_files/all?company_number=${id}`
    }

    return axios(config)
        .then(res => {
            dispatch(getInvertorInfoData({ type, data: res?.data }));
        })
        .catch(err => {
            console.error(err?.response?.data || err);
            dispatch(error());
        })
}