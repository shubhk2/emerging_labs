import axios from "axios";
import { CREATE_CHAT_HISTORY, ERROR, LOADING, REMOVE_CHAT_HISTORY, SET_CHAT_HISTORY, SET_CURRENT_LLM_RESPONSE } from "./actionTypes"

export const loading = () => {
    return {
        type: LOADING,
    };
}
export const error = () => {
    return {
        type: ERROR,
    }
}
export const createChatHistory = payload => {
    return {
        type: CREATE_CHAT_HISTORY,
        payload
    }
}
export const setChatHistory = (payload) => {
    return {
        type: SET_CHAT_HISTORY,
        payload
    }
}
export const removeChatHistory = (payload) => {
    return {
        type: REMOVE_CHAT_HISTORY,
        payload
    }
}
export const setCurrentLLMResponse = (payload) => {
    return {
        type: SET_CURRENT_LLM_RESPONSE,
        payload
    }
}

export const sendChatRequest = (query, chatId, historyLength, setSelectedChat) => dispatch => {
    dispatch(loading());
    
    if (!chatId) {
        const placeholderPayload = {
            chatId: historyLength,
            title: query || `Chat ${historyLength}`,
            historyData: {
                query: query,
                response: {
                    llm_response: "Loading response...",
                },
                timestamp: new Date().toISOString()
            }
        };
        dispatch(createChatHistory(placeholderPayload));
        setSelectedChat(historyLength);
    }

    const config = {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            "X-API-Key": process.env.REACT_APP_CODE
        },
        data: JSON.stringify({
            "user_query": query,
            "raw_only": false
        }),
        url: `${process.env.REACT_APP_BACKEND_URL || process.env.REACT_APP_BACKEND_URL_LOCAL}/copilot/ask`
    };

    return axios(config)
        .then(res => {
            dispatch(setCurrentLLMResponse(res.data));
            const payload = {
                chatId: chatId || historyLength,
                historyData: {
                    query: query,
                    response: res.data,
                    timestamp: new Date().toISOString()
                }
            };
            dispatch(setChatHistory(payload));
            return true;
        })
        .catch(err => {
            console.error(err?.response?.data || err);
            dispatch(error());
        });
}
