import { useState } from "react";
import { LineChart, Line, XAxis, Tooltip, ResponsiveContainer } from "recharts";
import { CiMenuKebab } from "react-icons/ci";
import "../../Styles/Components/Dashboard/PortfolioTimelineChart.css"

// Dummy full dataset (MAX range)
const fullData = [
    { date: "2021-01", invested: 100000, value: 102000, benchmark: 98000 },
    { date: "2021-06", invested: 120000, value: 125000, benchmark: 118000 },
    { date: "2022-01", invested: 150000, value: 148000, benchmark: 152000 },
    { date: "2022-06", invested: 170000, value: 165000, benchmark: 168000 },
    { date: "2023-01", invested: 200000, value: 215000, benchmark: 202000 },
    { date: "2023-06", invested: 230000, value: 245000, benchmark: 235000 },
    { date: "2024-01", invested: 250000, value: 265000, benchmark: 255000 },
    { date: "2024-06", invested: 280000, value: 295000, benchmark: 285000 },
    { date: "2025-06", invested: 300000, value: 325000, benchmark: 315000 },
];

// Timeframe filters
const timeframes = {
    "3M": 2,
    "6M": 3,
    "1Y": 4,
    "3Y": 6,
    "5Y": 8,
    Max: fullData.length,
};

export const PortfolioTimelineChart = () => {
    const [visibleLines, setVisibleLines] = useState({
        invested: true,
        value: true,
        benchmark: true,
    });
    const [timeframe, setTimeframe] = useState("Max");

    const slicedData = fullData.slice(-timeframes[timeframe]);

    const toggleLine = (line) => {
        setVisibleLines((prev) => ({
            ...prev,
            [line]: !prev[line],
        }));
    };

    return (
        <div className="portfolio-chart-container">
            <div className="head">
                <h2>Portfolio performance</h2>
                <div>
                    <CiMenuKebab />
                    <div className="portfolio-head-dropdown">
                        <div>
                            {Object.keys(timeframes).map((label) => (
                                <button key={label} onClick={() => setTimeframe(label)} disabled={timeframe === label}>
                                    {label}
                                </button>
                            ))}
                        </div>
                        <div>
                            {["invested", "value", "benchmark"].map((key) => (
                                <label key={key}>
                                    <input
                                        type="checkbox"
                                        checked={visibleLines[key]}
                                        onChange={() => toggleLine(key)}
                                    />
                                    {key === "invested"
                                        ? "Invested Amount"
                                        : key === "value"
                                            ? "Portfolio Value"
                                            : "Market Benchmark"}
                                </label>
                            ))}
                        </div>
                    </div>
                </div>
            </div>
            <hr />
            <div style={{ width: "100%", height: 300 }}>
                <ResponsiveContainer>
                    <LineChart data={slicedData}>
                        <XAxis dataKey="date" hide />
                        <Tooltip />
                        {visibleLines.invested && (
                            <Line
                                type="monotone"
                                dataKey="invested"
                                stroke="#10B981"
                                strokeWidth={3}
                                dot={false}
                            />
                        )}
                        {visibleLines.value && (
                            <Line
                                type="monotone"
                                dataKey="value"
                                stroke="#6366F1"
                                strokeWidth={3}
                                dot={false}
                            />
                        )}
                        {visibleLines.benchmark && (
                            <Line
                                type="monotone"
                                dataKey="benchmark"
                                stroke="#F59E0B"
                                strokeWidth={3}
                                dot={false}
                            />
                        )}
                    </LineChart>
                </ResponsiveContainer>
            </div>
        </div>
    );
};