import { useEffect, useState } from "react";

export const useThemeMode = () => {
    const [mode, setMode] = useState("system");

    useEffect(() => {
        const stored = localStorage.getItem("theme");

        if (stored === "light" || stored === "dark") {
            setMode(stored);
            document.documentElement.setAttribute("app-color-schema", stored);
        } else {
            // fallback to system preference
            const prefersDark = window.matchMedia("(prefers-color-scheme: dark)").matches;
            const systemMode = prefersDark ? "dark" : "light";
            setMode("system");
            document.documentElement.setAttribute("app-color-schema", systemMode);
        }
    }, []);

    const toggleTheme = () => {
        const current = document.documentElement.getAttribute("app-color-schema");
        const newMode = current === "dark" ? "light" : "dark";

        setMode(newMode);
        localStorage.setItem("theme", newMode);
        document.documentElement.setAttribute("app-color-schema", newMode);
    };

    return { mode, toggleTheme };
};
