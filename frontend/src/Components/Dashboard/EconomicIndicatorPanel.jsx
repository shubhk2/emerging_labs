import { useState } from "react";
import "../../Styles/Components/Dashboard/EconomicIndicatorPanel.css"

// Sample indicator data
const defaultIndicators = {
    India: [
        { name: "GDP", value: "6.8%", change: 0.3 },
        { name: "CPI", value: "5.2%", change: -0.4 },
        { name: "IIP", value: "2.7%", change: 0.1 },
    ],
    US: [
        { name: "GDP", value: "2.3%", change: 0.2 },
        { name: "CPI", value: "3.1%", change: -0.2 },
        { name: "Unemployment", value: "3.8%", change: -0.1 },
    ],
    Global: [
        { name: "Global GDP", value: "3.2%", change: 0.1 },
        { name: "Global Inflation", value: "6.0%", change: -0.5 },
    ],
};

export const EconomicIndicatorPanel = () => {
    const [region, setRegion] = useState("India");
    const [customMetrics, setCustomMetrics] = useState([]);
    const [newMetric, setNewMetric] = useState({ name: "", value: "", change: "" });
    const [visibleIndicators, setVisibleIndicators] = useState(
        defaultIndicators[region].map((_, idx) => idx)
    );

    const handleAddCustom = () => {
        if (newMetric.name && newMetric.value) {
            setCustomMetrics((prev) => [...prev, { ...newMetric, change: parseFloat(newMetric.change) || 0 }]);
            setNewMetric({ name: "", value: "", change: "" });
        }
    };

    const handleToggleIndicator = (index) => {
        setVisibleIndicators((prev) =>
            prev.includes(index) ? prev.filter((i) => i !== index) : [...prev, index]
        );
    };

    const regionData = defaultIndicators[region];

    return (
        <div className="economic-panel-container">
            <div>
                <select value={region} onChange={(e) => {
                    setRegion(e.target.value);
                    setVisibleIndicators(defaultIndicators[e.target.value].map((_, idx) => idx));
                }}>
                    <option value="India">India</option>
                    <option value="US">US</option>
                    <option value="Global">Global</option>
                </select>
            </div>
            <div>
                {regionData.map((indicator, index) =>
                    visibleIndicators.includes(index) ? (
                        <div key={index}>
                            <span>{indicator.name}</span>
                            <span> {indicator.value} </span>
                            <span style={{ color: indicator.change >= 0 ? "green" : "red" }}>
                                {indicator.change >= 0 ? "+" : ""}
                                {indicator.change}%
                            </span>
                            <button onClick={() => handleToggleIndicator(index)}>Hide</button>
                        </div>
                    ) : (
                        <div key={index}>
                            <span>{indicator.name} (Hidden)</span>
                            <button onClick={() => handleToggleIndicator(index)}>Show</button>
                        </div>
                    )
                )}
            </div>
            <hr />
            <div>
                <h4>Add Custom Metric</h4>
                <input
                    placeholder="Name"
                    value={newMetric.name}
                    onChange={(e) => setNewMetric({ ...newMetric, name: e.target.value })}
                />
                <input
                    placeholder="Value"
                    value={newMetric.value}
                    onChange={(e) => setNewMetric({ ...newMetric, value: e.target.value })}
                />
                <input
                    placeholder="% Change"
                    value={newMetric.change}
                    onChange={(e) => setNewMetric({ ...newMetric, change: e.target.value })}
                />
                <button onClick={handleAddCustom}>Add</button>
            </div>
            <div>
                {customMetrics.map((metric, index) => (
                    <div key={index}>
                        <span>{metric.name}</span>
                        <span> {metric.value} </span>
                        <span style={{ color: metric.change >= 0 ? "green" : "red" }}>
                            {metric.change >= 0 ? "+" : ""}
                            {metric.change}%
                        </span>
                    </div>
                ))}
            </div>
        </div>
    );
};