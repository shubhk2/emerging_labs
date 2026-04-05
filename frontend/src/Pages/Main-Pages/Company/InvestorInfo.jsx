import { useEffect, useMemo, useState } from 'react';
import '../../../Styles/Pages/Main-Pages/Company/InvestorInfo.css';
import { useDispatch, useSelector } from 'react-redux';
import { NavLink, useLocation, useNavigate, useParams } from 'react-router-dom';
import { getInvertorInfoDataFunc } from '../../../Redux/MainReducer/action';
import { formatLabel, shortifyDecimalValue } from '../../../Utils/utilities';
import ReactMarkdown from 'react-markdown';

export const InvestorInfo = () => {
    const { compId, type } = useParams();
    const location = useLocation();
    const navigate = useNavigate();
    const dispatch = useDispatch();
    const { isLoading, investorInfo } = useSelector(store => store.mainReducer);
    const [currentPageData, setCurrentPageData] = useState();
    const [selectedPDF, setSelectedPDF] = useState("");
    const [selectedMarkdownQuarter, setSelectedMarkdownQuarter] = useState(1);
    const types = useMemo(() => [
        "dividend",
        "shareholding_pattern",
        "insider_trading",
        "pledged_data",
        "cg_board_composition",
        "cg_committee_composition",
        "cg_board_meetings",
        "cg_committee_meetings",
        "rpt",
        "earning_calls",
        "quarterly",
        "annual"
    ], []);

    useEffect(() => {
        if (location.pathname.endsWith("/investor-info")) {
            navigate('dividend');
            return;
        }
        if (!types.includes(type)) {
            const newUrl = location.pathname.split("/");
            newUrl.pop();
            navigate(`${newUrl.join("/")}/dividend`);
            return;
        }
        dispatch(getInvertorInfoDataFunc(compId, type, selectedMarkdownQuarter));
    }, [type, types, compId, navigate, dispatch, location, selectedMarkdownQuarter])
    useEffect(() => {
        setCurrentPageData(investorInfo[type]);
    }, [investorInfo])
    useEffect(() => {
        if (currentPageData?.annual_file_ids?.length > 0) {
            setSelectedPDF(currentPageData?.annual_file_ids[0]);
        }
    }, [currentPageData?.annual_file_ids]);
    useEffect(() => {
        if (currentPageData?.quarterly_file_ids?.length > 0) {
            setSelectedPDF(currentPageData?.quarterly_file_ids[0]);
        }
    }, [currentPageData?.quarterly_file_ids]);
    if (!types.includes(type)) return null;
    return (
        <div className="investor-info-container">
            <div className='body'>
                <div className='tabs'>
                    {types.map((item) => (
                        <NavLink key={item} tabIndex={-1} to={`/company/${compId}/investor-info/${item}`}>
                            <button className='button-secondary'>{formatLabel(item)}</button>
                        </NavLink>
                    ))}
                </div>
                {
                    type === "earning_calls" ?
                        <div className='markdown-container'>
                            <div className='markdown-side-menu'>
                                <span onClick={() => setSelectedMarkdownQuarter(1)} className={selectedMarkdownQuarter === 1 ? 'active' : ''}>Quarter 1</span>
                                <span onClick={() => setSelectedMarkdownQuarter(2)} className={selectedMarkdownQuarter === 2 ? 'active' : ''}>Quarter 2</span>
                                <span onClick={() => setSelectedMarkdownQuarter(3)} className={selectedMarkdownQuarter === 3 ? 'active' : ''}>Quarter 3</span>
                                <span onClick={() => setSelectedMarkdownQuarter(4)} className={selectedMarkdownQuarter === 4 ? 'active' : ''}>Quarter 4</span>
                            </div>
                            <div className='markdown-viewer'>
                                <ReactMarkdown>{currentPageData?.text || ''}</ReactMarkdown>
                            </div>
                        </div>
                        : <></>
                }
                {
                    (type === "quarterly" || type === "annual") && investorInfo[type] ?
                        <div className='pdf-viewer-container'>
                            <div className='pdf-side-menu'>
                                {
                                    currentPageData.annual_file_ids ? currentPageData.annual_file_ids.map((file, id) => {

                                        return (
                                            <span key={file + id} className={selectedPDF === file ? 'active' : ''} onClick={() => setSelectedPDF(file)}>Year {(id === 0 && 2022) || (id === 1 && 2023) || (id === 2 && 2024)}</span>
                                        )
                                    }) : <></>
                                }
                                {
                                    currentPageData.quarterly_file_ids ? currentPageData.quarterly_file_ids.map((file, id) => {
                                        return (
                                            <span key={file + id} className={selectedPDF === file ? 'active' : ''} onClick={() => setSelectedPDF(file)}>Quarter {id + 1}</span>
                                        )
                                    }) : <></>
                                }
                            </div>
                            <div className="pdf-viewer">
                                <iframe title='PDF Viewer' src={`https://drive.google.com/file/d/${selectedPDF}/preview`} allow="autoplay"></iframe>
                                <a
                                    href={`https://drive.google.com/uc?export=download&id=${selectedPDF}`}
                                    download
                                    className="download-button"
                                    target="_blank"
                                    rel="noopener noreferrer"
                                >
                                    Download File
                                </a>
                            </div>
                        </div> : <></>
                }
                {
                    (type === "dividend" || type === "shareholding_pattern" || type === "insider_trading" || type === "pledged_data" ||
                        type === "cg_board_composition" || type === "cg_committee_composition" || type === "cg_board_meetings"
                        || type === "rpt" || type ==="cg_committee_meetings") && investorInfo[type] ?

                        <div className='table-container'
                            style={{
                            overflowX: 'auto',
                            maxWidth: '100%'
                            }}
                        >
                            <table className='table'
                                   style={{
                                    width: '100%',
                                    borderCollapse: 'collapse'
                                }}
                            >
                                {
                                    isLoading ?
                                        <tbody>
                                            <tr>
                                                <td>Getting Your Requested Data. Please Wait...</td>
                                            </tr>
                                        </tbody>
                                        :
                                        currentPageData?.data && currentPageData?.data.length ?
                                            <>
                                                <thead>
                                                    <tr>
                                                        {currentPageData?.headers?.map((date, index) => {
                                                            if (date === "Symbol") return;
                                                            return (
                                                                <th key={index}>{date}</th>
                                                            )
                                                        })}
                                                    </tr>
                                                </thead>
                                                <tbody>
                                                    {currentPageData?.data?.map((row, rowIndex) => {
                                                        return (
                                                            <tr key={rowIndex}>
                                                                {
                                                                    currentPageData?.headers?.map((head, headIndex) => {
                                                                        if (head === "Symbol") return;
                                                                        const value = row[head];
                                                                        return headIndex === 0 ?
                                                                            <td title={value} key={headIndex}>{shortifyDecimalValue(value) ?? '-'}</td>
                                                                            :
                                                                            <td key={headIndex}>{shortifyDecimalValue(value) ?? '-'}</td>
                                                                    })
                                                                }
                                                            </tr>
                                                        );
                                                    })}
                                                </tbody>
                                            </>
                                            :
                                            <tbody>
                                                <tr>
                                                    <td>No data available</td>
                                                </tr>
                                            </tbody>
                                }
                            </table>
                        </div> : <></>
                }
            </div>
        </div>
    )
}
