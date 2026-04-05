const RAW_API_BASE_URL =
    process.env.REACT_APP_BACKEND_URL ||
    process.env.REACT_APP_BACKEND_URL_LOCAL ||
    process.env.REACT_APP_BACKEND_BASE_URL ||
    "http://localhost:8000";

export const API_BASE_URL = RAW_API_BASE_URL.replace(/\/+$/, "");

export const API_KEY =
    process.env.REACT_APP_CODE ||
    process.env.REACT_APP_API_KEY ||
    "";

export const buildApiHeaders = (extraHeaders = {}) => {
    return {
        ...extraHeaders,
        ...(API_KEY ? { "X-API-Key": API_KEY } : {})
    };
};

