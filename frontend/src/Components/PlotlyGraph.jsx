import Plot from "react-plotly.js"

export const PlotlyGraph = ({ graphData }) => {
    const getCSSVariable = (name) => getComputedStyle(document.body).getPropertyValue(name).trim();
    const textColor = getCSSVariable('--text-primary');

    return (
        <Plot
            className='plot'
            data={graphData.data}
            layout={{
                ...graphData.layout,
                scrollZoom: true,
                paper_bgcolor: 'transparent',
                plot_bgcolor: 'transparent',
                font: {
                    color: textColor
                }
            }}
            config={{
                displayModeBar: false,
                scrollZoom: true,
                responsive: true,
            }}
            useResizeHandler={true}
            style={{ width: '100%', height: '100%' }}
        />
    )
}