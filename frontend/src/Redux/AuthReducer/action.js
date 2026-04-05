import axios from "axios";

export const loading = () => {
    return {
        type: "LOADING",
    };
}

export const error = () => {
    return {
        type: "ERROR",
    }
}

export const signin = (payload) => {
    return {
        type: "SIGNIN",
        payload,
    }
}

export const signup = (payload) => {
    return {
        type: "SIGNUP",
        payload,
    }
}

export const signout = () => {
    return {
        type: "SIGNOUT",
    }
}

export const signinFunc = (payload) => (dispatch) => {
    dispatch(loading());
    return axios.post(`${process.env.REACT_APP_BACKEND_BASE_URL}/login`, payload)
        .then((res) => {
            dispatch(signin(res.data));
        })
        .catch((err) => {
            dispatch(error());
        });
}

export const signupFunc = (payload) => (dispatch) => {
    dispatch(loading());
    return axios.post(`${process.env.REACT_APP_BACKEND_BASE_URL}/signup`, payload)
        .then((res) => {
            dispatch(signup(res.data));
        })
        .catch((err) => {
            dispatch(error());
        });
}

export const signoutFunc = () => (dispatch) => {
    dispatch(signout());
    localStorage.removeItem("token");
    localStorage.removeItem("userId");
    localStorage.removeItem("userName");
    localStorage.removeItem("userEmail");
    localStorage.removeItem("userPhoneNumber");
    localStorage.removeItem("userRole");
}