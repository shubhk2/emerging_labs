import { ERROR, GET_FINANCIAL_DATA, GET_OVERVIEW_DATA, LOADING, RESET_SEARCH_RESULTS, SEARCH_COMPANY, STOP_LOADING, REMOVE_ERROR, CREATE_TAB, REMOVE_TAB, SET_ACTIVE_TAB, GET_OVERVIEW_GRAPH_DATA, GET_INVESTOR_INFO_DATA } from "./actionTypes"

const initState = {
    isLoading: false,
    isError: false,
    overview: {
        data: {},
        graph: {}
    },
    financial: {
        balance_sheet: {},
        cashflow: {},
        profit_and_loss: {},
        ratio: {},
        data: []
    },
    investorInfo: {
        dividend: {},
        shareholding_pattern: {},
        earning_calls: {},
        quarterly: "",
        annual: ""
    },
    searchedCompanies: [],
    selectedCompany: {},
    userTabs: {}
}

export const reducer = (state = initState, { type, payload }) => {
    switch (type) {
        case LOADING:
            return {
                ...state,
                isLoading: true,
                isError: false
            };
        case STOP_LOADING:
            return {
                ...state,
                isLoading: false
            }
        case ERROR:
            return {
                ...state,
                isLoading: false,
                isError: true
            };
        case REMOVE_ERROR:
            return {
                ...state,
                isError: false
            }
        case GET_OVERVIEW_DATA:
            return {
                ...state,
                isLoading: false,
                isError: false,
                overview: {
                    ...state.overview,
                    data: payload.data || []
                }
            };
        case GET_OVERVIEW_GRAPH_DATA:
            return {
                ...state,
                isLoading: false,
                isError: false,
                overview: {
                    ...state.overview,
                    graph: (payload.data && JSON.parse(payload?.data?.plotly_json)) || {}
                }
            };
        case GET_FINANCIAL_DATA:
            return {
                ...state,
                isLoading: false,
                isError: false,
                financial: {
                    ...state.financial,
                    [payload.type]: payload.data || {}
                }
            };
        case GET_INVESTOR_INFO_DATA:
            return {
                ...state,
                isLoading: false,
                isError: false,
                investorInfo: { ...state.investorInfo, [payload.type]: payload.data }
            }
        case SEARCH_COMPANY:
            return {
                ...state,
                isLoading: false,
                isError: false,
                searchedCompanies: payload.results || []
            }
        case RESET_SEARCH_RESULTS:
            return {
                ...state,
                isLoading: false,
                isError: false,
                searchedCompanies: [],
                selectedCompany: {}
            }
        case CREATE_TAB:
            return {
                ...state,
                isLoading: false,
                isError: false,
                userTabs: {
                    ...state.userTabs,
                    [payload.companyId]: {
                        tabId: payload.tabId,
                        companyName: payload.companyName,
                        companyId: payload.companyId
                    }
                }
            }
        case REMOVE_TAB:
            const { [payload.companyId]: _, ...remainingTabs } = state.userTabs;
            return {
                ...state,
                isLoading: false,
                isError: false,
                userTabs: remainingTabs
            }
        case SET_ACTIVE_TAB:
            return {
                ...state,
                isLoading: false,
                isError: false,
                selectedCompany: state.userTabs[payload.companyId] || {}
            }
        default:
            return state
    }
}