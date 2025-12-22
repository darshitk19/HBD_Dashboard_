import React, { useEffect, useState } from "react";
import {
  Card,
  CardBody,
  Input,
  Typography,
  Button,
} from "@material-tailwind/react";
import axios from "axios";
import api from "../utils/Api";

const MisReportTable = () => {
  const [citySearch, setCitySearch] = useState("");
  const [categorySearch, setCategorySearch] = useState("");
  const [sortConfig, setSortConfig] = useState({ key: null, direction: "asc" });
  const [categories, setCategories] = useState([]);
  const [data, setData] = useState([]);

  // Transform raw data -> aggregated by city
  const transformData = (rawData) => {
    const cityMap = {};

    rawData.forEach((item) => {
      const city = item.city || "Unknown";
      const category = item.category || "Uncategorized";

      if (!cityMap[city]) {
        cityMap[city] = { city, total: 0 };
      }

      // total per city
      cityMap[city].total += 1;

      // category per city
      if (!cityMap[city][category]) {
        cityMap[city][category] = 0;
      }
      cityMap[city][category] += 1;
    });

    return Object.values(cityMap);
  };

  

  // Fetch data from backend
  const fetchData = async () => {
    try {
      const response = await api.get("/googlemap_data");
      const rawData = response.data;

      // categories for table headers
      const uniqueCategories = [...new Set(rawData.map((item) => item.category))];
      setCategories(uniqueCategories);

      // aggregate by city
      const aggregated = transformData(rawData);

      setData(aggregated);
    } catch (error) {
      console.error("Error fetching data:", error);
    }
  };

  useEffect(() => {
    fetchData();
  }, []);

  // Sorting logic (fixed for category columns too)
  const handleSort = (key) => {
    let direction = "asc";
    if (sortConfig.key === key && sortConfig.direction === "asc") {
      direction = "desc";
    }
    setSortConfig({ key, direction });
  };

  const sortedData = [...data].sort((a, b) => {
    if (!sortConfig.key) return 0;

    let aValue = a[sortConfig.key];
    let bValue = b[sortConfig.key];

    // ✅ Handle undefined category values
    if (aValue === undefined) aValue = 0;
    if (bValue === undefined) bValue = 0;

    if (aValue < bValue) return sortConfig.direction === "asc" ? -1 : 1;
    if (aValue > bValue) return sortConfig.direction === "asc" ? 1 : -1;
    return 0;
  });

  // Filter by city name
  const cityFilteredData = sortedData.filter((row) =>
    row.city.toLowerCase().includes(citySearch.toLowerCase())
  );

  //  Filter visible category columns
  const visibleCategories =
    categorySearch.trim() === ""
      ? categories
      : categories.filter((cat) =>
          cat.toLowerCase().includes(categorySearch.toLowerCase())
        );
  
  // 🔹 Compute grand totals across all cities
  const computeCategoryTotals = (data, categories) => {
    const totals = { total: 0 };
    categories.forEach((cat) => (totals[cat] = 0));

    data.forEach((row) => {
      totals.total += row.total;
      categories.forEach((cat) => {
        totals[cat] += row[cat] || 0;
      });
    });

    return totals;
  };

  const categoryTotals = computeCategoryTotals(cityFilteredData, visibleCategories);    

  //  Reset all filters
  const resetFilters = () => {
    setCitySearch("");
    setCategorySearch("");
    setSortConfig({ key: null, direction: "asc" });
  };

  return (
    <Card className="mt-6 shadow-xl border border-gray-100 rounded-2xl">
      <CardBody>
        {/* Header */}
       <div className="flex flex-wrap gap-4 justify-between items-center mb-6">
          <Typography variant="h4" className="font-bold text-gray-800">
            MIS Report - Listing Data
          </Typography>

          <div className="flex gap-3 items-center">
            {/* City Search */}
            <div className="w-56">
              <Input
                label="Search City"
                value={citySearch}
                onChange={(e) => setCitySearch(e.target.value)}
                crossOrigin=""
                className="h-12"
              />
            </div>

            {/* Category Search */}
            <div className="w-56">
              <Input
                label="Search Category"
                value={categorySearch}
                onChange={(e) => setCategorySearch(e.target.value)}
                crossOrigin=""
                className="h-12"
              />
            </div>

            {/* Reset Button */}
            <Button
              color="gray"
              variant="outlined"
              onClick={resetFilters}
              className="rounded-lg px-6"
            >
              Reset
            </Button>
          </div>
        </div>
        {/* Table */}
        <div className="overflow-x-auto rounded-xl border border-gray-200 shadow-sm">
          <table className="min-w-full text-sm text-left text-gray-700">
            <thead className="bg-gradient-to-r from-gray-100 to-gray-200 sticky top-0">
              <tr>
                <th
                  className="px-4 py-3 cursor-pointer font-semibold text-gray-900 border-b"
                  onClick={() => handleSort("city")}
                >
                  City{" "}
                  {sortConfig.key === "city" &&
                    (sortConfig.direction === "asc" ? "▲" : "▼")}
                </th>
                <th
                  className="px-4 py-3 cursor-pointer font-semibold text-gray-900 border-b"
                  onClick={() => handleSort("total")}
                >
                  Total Data{" "}
                  {sortConfig.key === "total" &&
                    (sortConfig.direction === "asc" ? "▲" : "▼")}
                </th>
                {visibleCategories.map((cat) => (
                  <th
                    key={cat}
                    className="px-4 py-3 cursor-pointer font-semibold text-gray-900 border-b"
                    onClick={() => handleSort(cat)}
                  >
                    {cat}{" "}
                    {sortConfig.key === cat &&
                      (sortConfig.direction === "asc" ? "▲" : "▼")}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {cityFilteredData.map((row, idx) => (
                <tr
                  key={idx}
                  className={`transition-colors ${
                    idx % 2 === 0 ? "bg-white" : "bg-gray-50"
                  } hover:bg-blue-50`}
                >
                  <td className="px-4 py-3 border-b font-medium text-gray-800">
                    {row.city}
                  </td>
                  <td className="px-4 py-3 border-b">{row.total}</td>
                  {visibleCategories.map((cat) => (
                    <td key={cat} className="px-4 py-3 border-b">
                      {row[cat] || 0}
                    </td>
                  ))}
                </tr>
              ))}
               {/* if no data found */}
              {cityFilteredData.length === 0 && (
                <tr>
                  <td
                    colSpan={visibleCategories.length + 2}
                    className="text-center py-6 text-gray-500"
                  >
                    No data found
                  </td>
                </tr>
              )}

              {/* Totals row (sticky footer) */}
              {cityFilteredData.length > 0 && (
                <tr className="bg-gray-200 font-semibold sticky bottom-0">
                  <td className="px-4 py-3 border-t text-gray-900">Total</td>
                  <td className="px-4 py-3 border-t">{categoryTotals.total}</td>
                  {visibleCategories.map((cat) => (
                    <td key={cat} className="px-4 py-3 border-t">
                      {categoryTotals[cat]}
                    </td>
                  ))}
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </CardBody>
    </Card>
  );
};

export default MisReportTable;
