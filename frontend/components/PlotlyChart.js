import React from "react";
import Plot from "react-plotly.js";

const PlotlyChart = ({ plotlyJson }) => {
  if (!plotlyJson) return null;
  let fig;
  try {
    fig = JSON.parse(plotlyJson);
  } catch (e) {
    return <div>Invalid chart data</div>;
  }
  return (
    <Plot
      data={fig.data}
      layout={fig.layout}
      config={fig.config}
      style={{ width: "100%", height: "100%" }}
      useResizeHandler={true}
    />
  );
};

export default PlotlyChart;
