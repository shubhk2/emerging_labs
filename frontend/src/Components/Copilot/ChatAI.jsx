import '../../Styles/Components/ChatAI.css';
import * as Dialog from "@radix-ui/react-dialog";
import { useDispatch, useSelector } from 'react-redux';
import { useEffect, useRef, useState } from 'react';
import { ChatMessageScreen } from './ChatMessageScreen';
import { removeChatHistory, sendChatRequest } from '../../Redux/ChatReducer/action';
import { FaTrashAlt } from "react-icons/fa";

export const ChatAI = () => {
    const dispatch = useDispatch();
    const { isLoading, chatHistory, chatHistoryMap } = useSelector((state) => state.chatReducer);
    const [selectedChat, setSelectedChat] = useState(0);
    const [chatQuery, setChatQuery] = useState('');
    const [placeholders, setPlaceholders] = useState({
        '~OVERVIEW_STATS_TABLE~': '',
        '~COMPARISON_TABLE~': '',
        '~SHAREHOLDING_TABLE~': '',
        '~RATIOS_TABLE~': '',
        '~COMPREHENSIVE_RATIOS_TABLE~': '',
        '~FINANCIAL_PARAMETERS_TABLE~': '',
        '~CHARTS_SECTION~': '',
        '~FINANCIAL_DATA_TABLE~': ''
    });

    const textareaRef = useRef(null);

    const handleInput = () => {
        const textarea = textareaRef.current;
        textarea.style.height = 'auto';
        textarea.style.height = `${textarea.scrollHeight}px`;
        setChatQuery(textarea.value);
    };

    const handleKeyDown = async (e) => {
        if (e.key === 'Enter' && !e.shiftKey) {
            e.preventDefault();
            dispatch(sendChatRequest(chatQuery, selectedChat, chatHistory.length + 1, setSelectedChat))
        }
    };

    useEffect(() => {
        setChatQuery('');
    }, [selectedChat]);

    return (
        <Dialog.Root className="chat-ai">
            <Dialog.Trigger asChild>
                <button className="chat-ai-Button button-secondary logo-button">
                    <svg fill="currentColor" xmlns="http://www.w3.org/2000/svg" version="1.0" width="100%" height="100%" viewBox="30 175 440 150" preserveAspectRatio="xMidYMid meet">
                        <g transform="translate(0.000000,500.000000) scale(0.100000,-0.100000)" stroke="none">
                            <path d="M850 2987 c-251 -86 -396 -347 -335 -606 42 -178 181 -322 364 -378 54 -16 221 -16 274 0 l38 12 -41 55 -40 54 -83 2 c-233 6 -382 158 -382 389 1 109 27 176 101 255 160 171 441 156 579 -31 108 -147 96 -370 -27 -496 -45 -46 -45 -42 16 -114 l29 -34 47 53 c149 167 181 383 85 578 -62 128 -172 223 -304 264 -91 29 -234 27 -321 -3z" />
                            <path d="M4260 2715 l0 -65 -30 0 c-29 0 -30 -2 -30 -45 0 -43 1 -45 30 -45 l30 0 0 -130 0 -130 60 0 60 0 0 130 0 130 30 0 c29 0 30 2 30 45 0 43 -1 45 -30 45 l-30 0 0 65 0 65 -60 0 -60 0 0 -65z" />
                            <path d="M936 2716 c-86 -32 -146 -120 -146 -213 1 -200 227 -306 372 -175 123 111 99 303 -46 379 -42 22 -133 26 -180 9z m123 -141 c32 -16 41 -33 41 -75 0 -42 -9 -59 -41 -75 -40 -21 -63 -19 -98 11 -25 20 -31 34 -31 64 0 30 6 44 31 64 35 30 58 32 98 11z" />
                            <path d="M2181 2650 c-49 -12 -83 -40 -108 -90 -42 -82 -20 -181 53 -236 32 -25 46 -29 100 -29 52 0 67 4 88 24 26 24 26 24 26 3 0 -20 5 -22 60 -22 l60 0 0 175 0 175 -60 0 c-54 0 -60 -2 -60 -21 l0 -20 -28 19 c-34 25 -86 33 -131 22z m130 -119 c19 -20 29 -40 29 -60 0 -34 -35 -79 -65 -82 -90 -12 -139 95 -68 151 36 28 69 25 104 -9z" />
                            <path d="M2708 2640 c-32 -17 -38 -18 -38 -5 0 12 -13 15 -60 15 l-60 0 0 -175 0 -175 60 0 60 0 0 110 c0 97 2 112 20 130 21 21 55 26 80 10 12 -7 16 -36 18 -130 l4 -120 55 0 54 0 -3 145 -3 145 -31 31 c-41 41 -102 48 -156 19z" />
                            <path d="M3509 2651 c-167 -33 -204 -259 -56 -338 21 -12 55 -18 102 -18 81 0 126 21 162 75 35 51 31 60 -29 60 -44 0 -55 -4 -73 -26 -38 -48 -111 -29 -130 34 l-6 22 135 0 136 0 0 30 c0 77 -57 141 -140 159 -28 6 -54 10 -58 10 -4 -1 -23 -4 -43 -8z m111 -96 c13 -14 21 -27 18 -30 -3 -3 -41 -5 -84 -3 l-79 3 27 28 c36 36 85 37 118 2z" />
                            <path d="M3896 2646 c-51 -19 -75 -44 -77 -81 -5 -66 28 -97 122 -111 93 -14 130 -58 64 -77 -40 -11 -85 6 -85 33 0 18 -6 20 -61 20 l-61 0 7 -27 c19 -78 68 -107 180 -107 118 0 167 32 167 109 0 63 -36 96 -117 110 -55 9 -105 25 -105 33 1 35 73 49 95 19 9 -12 28 -17 69 -17 31 0 56 2 56 5 0 21 -35 67 -60 80 -44 23 -147 29 -194 11z" />
                            <path d="M1620 2525 c0 -171 23 -221 107 -232 34 -4 67 5 126 35 4 2 7 -3 7 -12 0 -13 11 -16 55 -16 l55 0 0 175 0 175 -55 0 -55 0 -1 -87 c0 -49 -5 -104 -12 -123 -21 -59 -88 -74 -108 -23 -5 13 -9 71 -9 129 l0 104 -55 0 -55 0 0 -125z" />
                            <path d="M2940 2643 c1 -4 29 -82 64 -173 l63 -165 69 -3 70 -3 66 176 66 175 -58 0 -58 0 -37 -112 c-21 -62 -41 -117 -45 -121 -4 -5 -25 45 -47 110 l-40 118 -56 3 c-31 2 -57 -1 -57 -5z" />
                        </g>
                    </svg>
                </button>
            </Dialog.Trigger>
            <Dialog.Portal>
                <Dialog.Overlay className="chat-ai-DialogOverlay" />
                <Dialog.Content className="chat-ai-DialogContent">
                    <Dialog.Title className="chat-ai-DialogTitle">Quanvest Copilot</Dialog.Title>

                    <div className="chat-ai-copilot-container">
                        <div className='chat-ai-history'>
                            <button className={`button-secondary ${selectedChat === 0 && 'active'}`} onClick={() => setSelectedChat(0)}>New Chat</button>
                            <hr />
                            <div className='history-buttons'>
                                {
                                    chatHistory.map(chatId => {
                                        const chatHistoryTitle = chatHistoryMap[chatId]?.title || `Chat ${chatId}`;
                                        return (
                                            <button key={chatId} className={`button-secondary chat-history-tabs ${selectedChat === chatId && 'active'}`} onClick={() => setSelectedChat(chatId)}><span>{chatHistoryTitle}</span><FaTrashAlt className='remove-chat-history' onClick={e => { e.stopPropagation(); dispatch(removeChatHistory({ chatId })); setSelectedChat(0) }} /></button>
                                        )
                                    })
                                }
                            </div>
                        </div>
                        <div className='chat-ai-message-screen'>
                            <div className="chat-ai-message-preview">
                                {
                                    selectedChat === 0 ?
                                        <div className="chat-ai-welcome-message">
                                            <h2>Welcome to Quanvest Copilot</h2>
                                            <p>Start a new chat or select an existing one to view messages.</p>
                                            <p>Type your message in the input box below to interact with the AI.</p>
                                        </div>
                                        :
                                        <ChatMessageScreen chatId={selectedChat} />
                                }
                            </div>
                            <div className="chat-ai-message-input">
                                <textarea
                                    disable={isLoading} ref={textareaRef} id='message-input'
                                    rows={1} onKeyDown={handleKeyDown} onInput={handleInput} value={chatQuery}
                                    placeholder="Type your prompt here...">
                                </textarea>
                                <button
                                    className="chat-send-button"
                                    onClick={() => dispatch(sendChatRequest(chatQuery, selectedChat, chatHistory.length + 1, setSelectedChat))}
                                    disabled={isLoading || !chatQuery.trim()}
                                >
                                    Send
                                </button>
                            </div>
                        </div>
                    </div>

                </Dialog.Content>
            </Dialog.Portal>
        </Dialog.Root>
    )
}
