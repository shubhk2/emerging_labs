import { NavLink, useLocation, useNavigate, useParams } from 'react-router-dom';
import '../../../Styles/Pages/Main-Pages/Company/Financial.css';
import { useDispatch, useSelector } from 'react-redux';
import { useEffect, useMemo, useState } from 'react';
import { getFinancialDataFunc } from '../../../Redux/MainReducer/action';
import { ParameterChart } from '../../../Components/ParameterChart';
import { formatLabel, getRandomBrightColor, shortifyDecimalValue } from '../../../Utils/utilities';

export const Financial = () => {
    const { compId, type } = useParams();
    const navigate = useNavigate();
    const location = useLocation();
    const dispatch = useDispatch();
    const { isLoading, financial } = useSelector(store => store.mainReducer);
    const [decimalPos, setDecimalPos] = useState(1);
    const [currentPageData, setCurrentPageData] = useState({});
    const [generatedColors, setGeneratedColors] = useState(new Set());
    const [selectedParams, setSelectedParams] = useState(new Map());

    const types = useMemo(() => [
        "balance_sheet",
        "cashflow",
        "profit_and_loss",
        "ratio"
    ], []);

    useEffect(() => {
        if (location.pathname.endsWith("/financial")) {
            navigate('balance_sheet');
            return;
        }
        if (!types.includes(type)) {
            const newUrl = location.pathname.split("/");
            newUrl.pop();
            navigate(`${newUrl.join("/")}/balance_sheet`, { replace: true });
            return;
        }
        dispatch(getFinancialDataFunc(compId, type));
    }, [type, types, compId, navigate, dispatch, location]);

    useEffect(() => {
        type && financial[type] && setCurrentPageData(financial[type]);
    }, [financial, type])

    if (!types.includes(type)) return null;

    const handleRowToggle = (parameter, data) => {
        setSelectedParams(prev => {
            const newMap = new Map(prev);
            const newColorSet = new Set(generatedColors);
            if (newMap.has(parameter)) {
                const [oldColor] = newMap.get(parameter);
                newMap.delete(parameter);
                newColorSet.delete(oldColor);
            } else {
                let color;
                do {
                    color = getRandomBrightColor();
                } while (newColorSet.has(color));
                newColorSet.add(color);
                newMap.set(parameter, [color, data]);
            }
            setGeneratedColors(newColorSet);
            return newMap;
        });
    };

    return (
        <div className='financial-container'>
            <div className='body'>
                <div className="chart">
                    {selectedParams.size > 0 && (
                        <ParameterChart
                            selectedParams={selectedParams}
                            headers={currentPageData.headers}
                            handleRowToggle={handleRowToggle}
                        />
                    )}
                </div>
                <div className='tabs'>
                    {types.map((item) => (
                        <NavLink key={item} tabIndex={-1} to={`/company/${compId}/financial/${item}`}>
                            <button className='button-secondary'>{formatLabel(item)}</button>
                        </NavLink>
                    ))}
                </div>
                <div className='filters'>
                    <div className='decimal-position'>
                        <span>Decimal Position </span>
                        <span>
                            <button disabled={decimalPos <= 1} onClick={() => decimalPos > 1 && setDecimalPos(decimalPos - 1)}>.0</button>
                            <button disabled={decimalPos >= 8} onClick={() => decimalPos < 8 && setDecimalPos(decimalPos + 1)}>.00</button>
                        </span>
                    </div>
                </div>
                <div className='table-container'>
                    <table className='table'>
                        {
                            isLoading ?
                                <tbody>
                                    <tr>
                                        <td>Getting Your Requested Data. Please Wait...</td>
                                    </tr>
                                </tbody>
                                :
                                currentPageData.data && currentPageData.data.length ?
                                    <>
                                        <thead>
                                            <tr>
                                                {currentPageData?.headers?.map((date, index) => (
                                                    <th key={index}>{date}</th>
                                                ))}
                                            </tr>
                                        </thead>
                                        <tbody>
                                            {currentPageData?.data?.map((row, rowIndex) => {
                                                return (
                                                    <tr key={rowIndex} onClick={() => handleRowToggle(row[currentPageData?.headers[0]], row)}>
                                                        {
                                                            currentPageData?.headers?.map((head, headIndex) => {
                                                                if (headIndex === 0 && (head === "Account" || head === "Ratio")) {
                                                                    return (
                                                                        <td key={headIndex}>
                                                                            <input className="table-row-checkbox" type="checkbox" style={{ '--checkbox-color': selectedParams.has(row[head]) && selectedParams.get(row[head])[0] }} checked={selectedParams.has(row[head])} readOnly /> {formatLabel(row[head])}
                                                                        </td>
                                                                    )
                                                                } else {
                                                                    return (
                                                                        <td key={headIndex}>{shortifyDecimalValue(row[head], decimalPos) ?? '-'}</td>
                                                                    )
                                                                }
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
                </div>
            </div>
        </div>
    );
};