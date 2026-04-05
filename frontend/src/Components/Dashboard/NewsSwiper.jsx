import { useState } from "react";
import { Swiper, SwiperSlide } from "swiper/react";
import { Autoplay, Navigation } from "swiper/modules";
import { FaBell } from "react-icons/fa";
import { HiMiniBellSnooze } from "react-icons/hi2";
import "swiper/css";
import "swiper/css/navigation";
import "../../Styles/Components/Dashboard/NewsSwiper.css"
import { IoCloseCircle } from "react-icons/io5";

const newsItems = [
    "TCS announces strategic partnership with Tesla… Shares surge 4.2%",
    "Infosys Q4 net profit up 15%; beats estimates",
    "Reliance plans $10B green energy investment",
];

const alerts = [
    { id: 1, message: "WIPRO hit your price trigger", unread: true },
    { id: 2, message: "INFY posted Q1 results", unread: false },
];

export const NewsSwiper = () => {
    const [showDropdown, setShowDropdown] = useState(false);
    const unreadCount = alerts.filter(a => a.unread).length;

    return (
        <div className="dash-news-container">
            <Swiper className="swiper-container" modules={[Autoplay, Navigation]} autoplay={{ delay: 5000, disableOnInteraction: false }} navigation loop>
                {newsItems.map((news, idx) => (
                    <SwiperSlide key={idx}>
                        {news}
                    </SwiperSlide >
                ))}
            </Swiper>
            <div className="news-alert">
                <button onClick={() => setShowDropdown(!showDropdown)}>
                    <FaBell size={20} />
                    {unreadCount > 0 && (
                        <>
                            {unreadCount}
                        </>
                    )}
                </button>
                {showDropdown && (
                    <div className="news-alert-dropdown">
                        {alerts.map((alert) => (
                            <div className="news-alert-dropdown-item" key={alert.id}>
                                <span>
                                    {alert.message}
                                </span>
                                <span>
                                    <button><IoCloseCircle /></button>
                                    <button><HiMiniBellSnooze /></button>
                                </span>
                            </div>
                        ))}
                    </div>
                )}
            </div>
        </div>
    );
};