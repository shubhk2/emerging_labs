import React, { useState } from "react";
import axios from "axios";
import PlotlyChart from "./PlotlyChart";
//THESE FILES ARE FOR HIMANSHU BHAIYA TO UNDERSTAND THE WORKING OF PLOTLY
const ChartContainer = () => {
  const [plotlyJson, setPlotlyJson] = useState(null);

  const fetchChart = async () => {
    // Example payload, replace with actual user selections
    const payload = {
      company_numbers: [1, 2],
      parameters: ["Net Profit"],
      start_year: 2020,
      end_year: 2023,
      chart_type: "line"
    };
    const res = await axios.post("http://ec2-13-60-21-61.eu-north-1.compute.amazonaws.com:8000/charts/parameters", payload);
    setPlotlyJson(res.data.plotly_json);
  };
  return (
    <div>
      <button onClick={fetchChart}>Generate Chart</button>
      <PlotlyChart plotlyJson={plotlyJson} />
    </div>
  );
};

export default ChartContainer;
