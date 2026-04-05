import { ERROR, LOADING, SIGNIN, SIGNOUT, SIGNUP } from "./actionTypes"

const initState = {
    isAuth: false,
    isLoading: false,
    isError: false,
    token: null,
    userData: []
}

export const reducer = (state = initState, { type, payload }) => {
    switch (type) {
        case LOADING:
            return {
                ...state,
                isLoading: true,
            }
        case ERROR:
            return {
                ...state,
                isLoading: false,
                isError: true,
            }
        case SIGNIN:
            return {
                ...state,
                isLoading: false,
                isAuth: true,
                token: payload.token,
                userData: payload.userData,
            }
        case SIGNUP:
            return {
                ...state,
                isLoading: false,
                isAuth: true,
                token: payload.token,
                userData: payload.userData,
            }
        case SIGNOUT:
            return {
                ...state,
                isLoading: false,
                isAuth: false,
                token: null,
                userData: [],
            }
        default:
            return state
    }
}