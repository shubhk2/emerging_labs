import { useState, useMemo } from "react";
import "../../Styles/Components/Dashboard/EconomicIndicatorPanel.css";

const sampleData = [
    {
        assetType: "Equity",
        name: "WIPRO",
        category: "Large-cap",
        quantity: 50,
        price: 450,
        value: 22500,
        status: "Buy",
        date: "2023-10-05",
    },
    {
        assetType: "ETF",
        name: "Nifty 50 ETF",
        category: "Index",
        quantity: 30,
        price: 200,
        value: 6000,
        status: "Hold",
        date: "2022-08-15",
    },
    {
        assetType: "MF",
        name: "SBI Bluechip Fund",
        category: "Large-cap",
        quantity: 100,
        price: 60,
        value: 6000,
        status: "Dividend",
        date: "2023-04-10",
    },
];

export const HoldingsTable = () => {
    const [search, setSearch] = useState("");
    const [sortField, setSortField] = useState("date");
    const [sortOrder, setSortOrder] = useState("desc");
    const [assetFilter, setAssetFilter] = useState("All");
    const [statusFilter, setStatusFilter] = useState("All");

    const filteredData = useMemo(() => {
        return sampleData
            .filter((item) => {
                const matchesSearch = item.name.toLowerCase().includes(search.toLowerCase());
                const matchesAsset = assetFilter === "All" || item.assetType === assetFilter;
                const matchesStatus = statusFilter === "All" || item.status === statusFilter;
                return matchesSearch && matchesAsset && matchesStatus;
            })
            .sort((a, b) => {
                if (sortField === "date") {
                    return sortOrder === "asc"
                        ? new Date(a.date) - new Date(b.date)
                        : new Date(b.date) - new Date(a.date);
                } else {
                    return sortOrder === "asc"
                        ? a[sortField] - b[sortField]
                        : b[sortField] - a[sortField];
                }
            });
    }, [search, sortField, sortOrder, assetFilter, statusFilter]);

    const exportToCSV = () => {
        const header = Object.keys(sampleData[0]);
        const rows = filteredData.map((row) => header.map((h) => row[h]));
        const csv = [header.join(","), ...rows.map((r) => r.join(","))].join("\n");

        const blob = new Blob([csv], { type: "text/csv" });
        const url = URL.createObjectURL(blob);

        const a = document.createElement("a");
        a.href = url;
        a.download = "holdings.csv";
        a.click();
        URL.revokeObjectURL(url);
    };

    return (
        <div>
            <h2>Holdings</h2>

            <div>
                <input
                    type="text"
                    placeholder="Search by name"
                    value={search}
                    onChange={(e) => setSearch(e.target.value)}
                />

                <select value={assetFilter} onChange={(e) => setAssetFilter(e.target.value)}>
                    <option value="All">All Assets</option>
                    <option value="Equity">Equity</option>
                    <option value="ETF">ETF</option>
                    <option value="MF">MF</option>
                </select>

                <select value={statusFilter} onChange={(e) => setStatusFilter(e.target.value)}>
                    <option value="All">All Status</option>
                    <option value="Buy">Buy</option>
                    <option value="Hold">Hold</option>
                    <option value="Dividend">Dividend</option>
                </select>

                <select value={sortField} onChange={(e) => setSortField(e.target.value)}>
                    <option value="date">Date</option>
                    <option value="value">Value</option>
                    <option value="category">Category</option>
                </select>

                <button onClick={() => setSortOrder(sortOrder === "asc" ? "desc" : "asc")}>
                    Sort: {sortOrder}
                </button>

                <button onClick={exportToCSV}>Export CSV</button>
            </div>

            <table>
                <thead>
                    <tr>
                        <th>Asset Type</th>
                        <th>Name</th>
                        <th>Category</th>
                        <th>Quantity</th>
                        <th>Price</th>
                        <th>Value</th>
                        <th>Status</th>
                        <th>Date</th>
                        <th>Action</th>
                    </tr>
                </thead>
                <tbody>
                    {filteredData.map((row, i) => (
                        <tr key={i}>
                            <td>{row.assetType}</td>
                            <td>{row.name}</td>
                            <td>{row.category}</td>
                            <td>{row.quantity}</td>
                            <td>{row.price}</td>
                            <td>{row.value}</td>
                            <td>{row.status}</td>
                            <td>{row.date}</td>
                            <td>
                                <button>Details</button>
                            </td>
                        </tr>
                    ))}
                </tbody>
            </table>
        </div>
    );
};