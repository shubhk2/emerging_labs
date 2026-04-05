import { useState } from "react";
import { PieChart, Pie, Tooltip, Cell, ResponsiveContainer } from "recharts";
import "../../Styles/Components/Dashboard/AssetAllocationPieChart.css"

// Colors for slices
const COLORS = ["#6366F1", "#10B981", "#F59E0B", "#EF4444", "#3B82F6", "#8B5CF6", "#14B8A6", "#A3E635"];

// Main categories
const overallData = [
    { name: "Equity", value: 400000 },
    { name: "Mutual Funds", value: 250000 },
    { name: "ETFs", value: 150000 },
    { name: "Bonds", value: 80000 },
    { name: "Commodities", value: 60000 },
    { name: "REITs", value: 50000 },
    { name: "Cash", value: 30000 },
];

// Drill-down data
const drillDownViews = {
    Equity: [
        { name: "Tech", value: 180000 },
        { name: "Auto", value: 100000 },
        { name: "Pharma", value: 70000 },
        { name: "FMCG", value: 50000 },
    ],
    "Mutual Funds": [
        { name: "Large-cap", value: 100000 },
        { name: "Mid-cap", value: 70000 },
        { name: "Hybrid", value: 50000 },
        { name: "Thematic", value: 30000 },
    ],
    ETFs: [
        { name: "Index ETFs", value: 90000 },
        { name: "Sectoral", value: 40000 },
        { name: "International", value: 20000 },
    ],
    Commodities: [
        { name: "Gold", value: 30000 },
        { name: "Silver", value: 15000 },
        { name: "Copper", value: 10000 },
        { name: "Others", value: 5000 },
    ],
};

// Tooltip component
const CustomTooltip = ({ active, payload, total }) => {
    if (active && payload && payload.length) {
        const { name, value } = payload[0].payload;
        const percent = ((value / total) * 100).toFixed(1);
        return (
            <div>
                <div><strong>{name}</strong></div>
                <div>₹{value.toLocaleString()}</div>
                <div>{percent}% of total</div>
            </div>
        );
    }
    return null;
};

export const AssetAllocationPieChart = () => {
    const [selectedView, setSelectedView] = useState("Overall");

    const data = selectedView === "Overall" ? overallData : drillDownViews[selectedView] || [];
    const total = data.reduce((sum, item) => sum + item.value, 0);

    return (
        <div className="asset-allocation-chart-container">
            <div>
                <select value={selectedView} onChange={(e) => setSelectedView(e.target.value)}>
                    <option value="Overall">Overall</option>
                    <option value="Equity">Equity View</option>
                    <option value="Mutual Funds">Mutual Fund View</option>
                    <option value="ETFs">ETF View</option>
                    <option value="Commodities">Commodities View</option>
                </select>
            </div>
            <div style={{ width: "100%", height: 300 }}>
                <ResponsiveContainer>
                    <PieChart>
                        <Pie
                            data={data}
                            dataKey="value"
                            nameKey="name"
                            cx="50%"
                            cy="50%"
                            outerRadius={100}
                            label
                        >
                            {data.map((entry, index) => (
                                <Cell key={index} fill={COLORS[index % COLORS.length]} />
                            ))}
                        </Pie>
                        <Tooltip content={<CustomTooltip total={total} />} />
                    </PieChart>
                </ResponsiveContainer>
            </div>
        </div>
    );
};