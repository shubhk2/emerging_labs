import { Swiper, SwiperSlide } from "swiper/react";
import { FaStar } from "react-icons/fa";
import "swiper/css";
import { useState } from "react";
import "../../Styles/Components/Dashboard/WatchlistSwiper.css"
import { Line, LineChart, ResponsiveContainer, YAxis } from "recharts";

const sampleStocks = [
    {
        ticker: "1WIPRO",
        name: "Wipro Ltd",
        ltp: 453.8,
        change: 1.2,
        chart: [
            { time: 1, price: 420 },
            { time: 2, price: 443 },
            { time: 3, price: 435 },
            { time: 4, price: 450 },
            { time: 5, price: 442 },
            { time: 6, price: 474 },
        ],
    },
    {
        ticker: "2INFY",
        name: "Infosys Ltd",
        ltp: 1500.3,
        change: -0.8,
        chart: [
            { time: 1, price: 1590 },
            { time: 2, price: 1578 },
            { time: 3, price: 1595 },
            { time: 4, price: 1543 },
            { time: 5, price: 1561 },
            { time: 6, price: 1500 },
        ],
    },
    {
        ticker: "3RELIANCE",
        name: "Reliance Industries",
        ltp: 2802.6,
        change: 2.4,
        chart: [
            { time: 1, price: 2750 },
            { time: 2, price: 2765 },
            { time: 3, price: 2770 },
            { time: 4, price: 2785 },
            { time: 5, price: 2795 },
            { time: 6, price: 2802 },
        ],
    },
    {
        ticker: "4WIPRO",
        name: "Wipro Ltd",
        ltp: 453.8,
        change: 1.2,
        chart: [
            { time: 1, price: 420 },
            { time: 2, price: 443 },
            { time: 3, price: 435 },
            { time: 4, price: 450 },
            { time: 5, price: 442 },
            { time: 6, price: 474 },
        ],
    },
    {
        ticker: "5INFY",
        name: "Infosys Ltd",
        ltp: 1500.3,
        change: -0.8,
        chart: [
            { time: 1, price: 1590 },
            { time: 2, price: 1578 },
            { time: 3, price: 1595 },
            { time: 4, price: 1543 },
            { time: 5, price: 1561 },
            { time: 6, price: 1500 },
        ],
    },
    {
        ticker: "6RELIANCE",
        name: "Reliance Industries",
        ltp: 2802.6,
        change: 2.4,
        chart: [
            { time: 1, price: 2750 },
            { time: 2, price: 2765 },
            { time: 3, price: 2770 },
            { time: 4, price: 2785 },
            { time: 5, price: 2795 },
            { time: 6, price: 2802 },
        ],
    },
    {
        ticker: "7WIPRO",
        name: "Wipro Ltd",
        ltp: 453.8,
        change: 1.2,
        chart: [
            { time: 1, price: 420 },
            { time: 2, price: 443 },
            { time: 3, price: 435 },
            { time: 4, price: 450 },
            { time: 5, price: 442 },
            { time: 6, price: 474 },
        ],
    },
    {
        ticker: "8INFY",
        name: "Infosys Ltd",
        ltp: 1500.3,
        change: -0.8,
        chart: [
            { time: 1, price: 1590 },
            { time: 2, price: 1578 },
            { time: 3, price: 1595 },
            { time: 4, price: 1543 },
            { time: 5, price: 1561 },
            { time: 6, price: 1500 }
        ],
    },
    {
        ticker: "9RELIANCE",
        name: "Reliance Industries",
        ltp: 2802.6,
        change: 2.4,
        chart: [
            { time: 1, price: 2750 },
            { time: 2, price: 2765 },
            { time: 3, price: 2770 },
            { time: 4, price: 2785 },
            { time: 5, price: 2795 },
            { time: 6, price: 2802 },
        ],
    },
];

export const WatchlistSwiper = () => {
    const [filter, setFilter] = useState("All");

    const filteredStocks = sampleStocks.filter((stock) => {
        if (filter === "Gainers") return stock.change > 0;
        if (filter === "Losers") return stock.change < 0;
        return true;
    });

    return (
        <div className="dash-watchlist-container">
            <div>
                <div><b>📈 Watchlist</b></div>
                <div>
                    {["All", "Gainers", "Losers"].map((cat) => (
                        <button key={cat} onClick={() => setFilter(cat)}>
                            {cat}
                        </button>
                    ))}
                </div>
            </div>
            <Swiper slidesPerView="5" spaceBetween={50}>
                {filteredStocks.map((stock) => (
                    <SwiperSlide key={stock.ticker} className="watchlist-slide">
                        <>
                            <div>
                                <div>{stock.name}</div>
                                <button>
                                    <FaStar />
                                </button>
                            </div>
                            <div>
                                <ResponsiveContainer height={"100%"} width={"100%"}>
                                    <LineChart data={stock.chart}>
                                        <YAxis
                                            type="number"
                                            domain={[
                                                (dataMin) => Math.floor(dataMin - 5),
                                                (dataMax) => Math.ceil(dataMax + 5)
                                            ]}
                                            hide
                                        />
                                        <Line
                                            type="linear"
                                            dataKey="price"
                                            stroke={stock.change >= 0 ? "#22c55e" : "#ef4444"}
                                            strokeWidth={2}
                                            dot={false}
                                        />
                                    </LineChart>
                                </ResponsiveContainer>
                            </div>
                            <button>
                                Analyze
                            </button>
                        </>
                    </SwiperSlide>
                ))}
            </Swiper>
        </div>
    );
};