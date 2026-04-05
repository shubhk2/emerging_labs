import '../../../Styles/Pages/Main-Pages/Company/Forecasting.css';
import { MdKeyboardArrowDown } from "react-icons/md";
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';

export const Forecasting = () => {
    const data = [
        { name: 'Q1', revenue: 4000, profit: 2400 },
        { name: 'Q2', revenue: 3000, profit: 1398 },
        { name: 'Q3', revenue: 2000, profit: 9800 },
        { name: 'Q4', revenue: 2780, profit: 3908 },
        { name: 'Q5', revenue: 4000, profit: 2400 },
        { name: 'Q6', revenue: 3000, profit: 1398 },
        { name: 'Q7', revenue: 2000, profit: 9800 },
        { name: 'Q8', revenue: 2780, profit: 3908 },
        { name: 'Q9', revenue: 4000, profit: 2400 },
        { name: 'Q10', revenue: 3000, profit: 1398 },
        { name: 'Q11', revenue: 2000, profit: 9800 },
        { name: 'Q12', revenue: 2780, profit: 3908 },
    ];
    return (
        <div className="forecasting-container">
            <div className='forecasting-header'>
                <button>3-Statement</button>
                <button>DCF</button>
                <button>ML Forecast</button>
                <button>Monte Carlo</button>
                <button>Sensitivity</button>
            </div>
            <div className='forecasting-body'>
                <h3>Discounted Cash Flow</h3>
                <div>
                    <p><MdKeyboardArrowDown /> Input Value</p>
                    <span>Revenue Growth Rate:</span>
                    <span>WACC (Discount Rate):</span>
                    <span>Forecast Period:</span>
                    <span>EBIT Margin:</span>
                    <span>Terminal Growth Rate:</span>
                    <span>Tax Rate:</span>
                </div>
                <div>
                    <p><MdKeyboardArrowDown /> Forecasted Value</p>
                    <div className='box-1'>
                        <span>Intrinsic Value:</span><span>Rs 825/share</span>
                        <span>Current Market Price:</span><span>Rs 720</span>
                        <span>Upside Potencial:</span><span>+14.6%</span>
                        <span>Valuation Range (Base Case):</span><span>Rs 780 - Rs 860</span>
                    </div>
                    <div className='box-2'>
                        The intrinsic value of the stock is estimated at Rs 825 per share, compared to the current market price of Rs 720, indicating a potencial upside of 14.6%. Based on the assumptions, the valuation range lies between Rs 780 and Rs 860. Model confidence is medius, with editable inputs for further analysis.
                    </div>
                    <div className='box-3'>
                        <ResponsiveContainer width="100%" height={300}>
                            <LineChart data={data}>
                                <CartesianGrid strokeDasharray="3 3" />
                                <XAxis dataKey="name" />
                                <YAxis />
                                <Tooltip />
                                <Legend />
                                <Line type="monotone" dataKey="revenue" stroke="#8884d8" />
                                <Line type="monotone" dataKey="profit" stroke="#82ca9d" />
                            </LineChart>
                        </ResponsiveContainer>
                    </div>
                </div>
            </div>
        </div>
    );
};