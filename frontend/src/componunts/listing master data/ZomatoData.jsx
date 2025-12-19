import React, { useEffect, useMemo, useState } from "react";
import {
  Button,
  Card,
  CardBody,
  CardHeader,
  Typography,
  Input,
  Spinner,
} from "@material-tailwind/react";

import {
  MagnifyingGlassIcon,
  ChevronUpDownIcon,
  ChevronDownIcon,
} from "@heroicons/react/24/solid";


import { zomatoData } from "../../data/ZomatoJSON"; 
import * as XLSX from "xlsx/dist/xlsx.full.min.js";

const defaultColumns = [
  { key: "res_name", label: "Restaurant Name", width: 220 },
  { key: "address", label: "Address", width: 300 },
  { key: "cuisines", label: "Cuisines", width: 200 },
  { key: "cost_for_two", label: "Cost (Two)", width: 120 },
  { key: "delivery_rating", label: "Delivery ⭐", width: 120 },
  { key: "dining_rating", label: "Dining ⭐", width: 120 },
  { key: "is_open", label: "Status", width: 100 },
  { key: "city", label: "City", width: 140 },
  { key: "contact", label: "Contact", width: 150 },
  { key: "url", label: "Zomato Link", width: 200 },
];

// Convert JSON to CSV
const convertToCSV = (arr) => {
  if (!arr?.length) return "";
  const headers = Object.keys(arr[0]);
  const rows = arr.map((r) =>
    headers.map((h) => `"${String(r[h] ?? "").replace(/"/g, "'")}"`).join(",")
  );
  return [headers.join(","), ...rows].join("\n");
};

const ZomatoData = () => {
  const [loading, setLoading] = useState(true);
  const [fullData, setFullData] = useState([]);
  const [pageData, setPageData] = useState([]);
  const [currentPage, setCurrentPage] = useState(1);
  const [total, setTotal] = useState(0);
  const limit = 10;

  const [search, setSearch] = useState("");
  const [cuisineSearch, setCuisineSearch] = useState("");

  const [sortField, setSortField] = useState(null);
  const [sortOrder, setSortOrder] = useState("asc");

  const [columns] = useState(defaultColumns);

  // Load Data
  useEffect(() => {
    setLoading(true);
    setTimeout(() => {
      // Fallback to empty array if zomatoData isn't loaded yet
      const data = zomatoData || [];
      setFullData(data);
      setTotal(data.length);
      setLoading(false);
    }, 300);
  }, []);

  // Filter Data
  const filteredData = useMemo(() => {
    let data = [...fullData];
    const safeValue = (value) => String(value ?? "").toLowerCase();

    if (search) {
      const s = search.toLowerCase();
      data = data.filter((x) => safeValue(x.res_name).includes(s));
    }

    if (cuisineSearch) {
      const s = cuisineSearch.toLowerCase();
      data = data.filter((x) => safeValue(x.cuisines).includes(s));
    }

    return data;
  }, [fullData, search, cuisineSearch]);

  // Sort Data
  const sortedData = useMemo(() => {
    if (!sortField) return filteredData;

    return [...filteredData].sort((a, b) => {
      const A = a[sortField];
      const B = b[sortField];

      // Numeric sorting for ratings and cost
      if (typeof A === 'number' && typeof B === 'number') {
        return sortOrder === "asc" ? A - B : B - A;
      }

      const strA = String(A ?? "").toLowerCase();
      const strB = String(B ?? "").toLowerCase();
      if (strA === strB) return 0;
      return sortOrder === "asc" ? (strA > strB ? 1 : -1) : (strA < strB ? 1 : -1);
    });
  }, [filteredData, sortField, sortOrder]);

  // Pagination
  useEffect(() => {
    const start = (currentPage - 1) * limit;
    setPageData(sortedData.slice(start, start + limit));
    setTotal(sortedData.length);
  }, [sortedData, currentPage]);

  const totalPages = Math.max(1, Math.ceil(total / limit));

  // Sorting Handler
  const toggleSort = (field) => {
    if (sortField === field) {
      setSortOrder((o) => (o === "asc" ? "desc" : "asc"));
    } else {
      setSortField(field);
      setSortOrder("asc");
    }
  };

  const downloadExcel = (currentOnly = false) => {
    const arr = currentOnly ? pageData : fullData;
    if (!arr.length) return;
    const ws = XLSX.utils.json_to_sheet(arr);
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, "Zomato");
    XLSX.writeFile(wb, currentOnly ? "zomato_page.xlsx" : "zomato_all.xlsx");
  };

  return (
    <div className="min-h-screen mt-8 mb-12 px-4 rounded bg-white text-black">
      <div className="flex justify-between items-center mb-4">
        <Typography variant="h4" className="pb-2 text-red-600">
          Zomato Restaurant Data
        </Typography>

        <div className="flex items-center gap-2">
          <Button size="sm" onClick={() => downloadExcel(false)} className="bg-red-500">
            Excel All
          </Button>
          <Button size="sm" onClick={() => downloadExcel(true)} className="bg-red-400">
            Excel Page
          </Button>
        </div>
      </div>

      <Card className="bg-white text-black border">
        <CardHeader className="flex flex-wrap items-center justify-between gap-3 p-4 bg-gray-50">
          <div className="flex gap-3 items-center flex-wrap">
            <Input 
              label="Search Restaurant..." 
              value={search} 
              onChange={(e) => setSearch(e.target.value)} 
            />
            <Input
              label="Search Cuisine..."
              value={cuisineSearch}
              onChange={(e) => setCuisineSearch(e.target.value)}
              icon={<MagnifyingGlassIcon className="h-5 w-5" />}
            />
          </div>

          <div className="flex gap-2 items-center">
            <div className="text-sm font-medium">Page {currentPage} / {totalPages}</div>
            <Button size="sm" variant="outlined" disabled={currentPage === 1} onClick={() => setCurrentPage((p) => p - 1)}>Prev</Button>
            <Button size="sm" variant="outlined" disabled={currentPage === totalPages} onClick={() => setCurrentPage((p) => p + 1)}>Next</Button>
          </div>
        </CardHeader>

        <CardBody className="p-0 overflow-x-auto">
          {loading ? (
            <div className="flex justify-center py-10"><Spinner className="h-10 w-10 text-red-500" /></div>
          ) : (
            <table className="w-full table-fixed border-collapse min-w-[1500px]">
              <thead className="sticky top-0 z-20 border-b bg-red-50">
                <tr>
                  {columns.map((col) => (
                    <th key={col.key} style={{ width: col.width }} className="px-3 py-3 text-left">
                      <div className="flex items-center gap-2 cursor-pointer" onClick={() => toggleSort(col.key)}>
                        <span className="capitalize text-xs font-bold text-red-900">{col.label}</span>
                        {sortField === col.key ? (
                          sortOrder === "asc" ? <ChevronUpDownIcon className="h-4" /> : <ChevronDownIcon className="h-4" />
                        ) : (
                          <ChevronUpDownIcon className="h-4 opacity-40" />
                        )}
                      </div>
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {pageData.length === 0 ? (
                  <tr><td colSpan={columns.length} className="text-center p-6">No restaurants found</td></tr>
                ) : (
                  pageData.map((row, idx) => (
                    <tr key={idx} className="border-b hover:bg-red-50/30 transition-colors">
                      {columns.map((col) => (
                        <td key={col.key} className="px-3 py-3 text-sm truncate">
                          {col.key === "url" ? (
                            <a href={row[col.key]} target="_blank" rel="noreferrer" className="text-blue-500 underline">Link</a>
                          ) : col.key === "cost_for_two" ? (
                             `₹${row[col.key]}`
                          ) : (
                            String(row[col.key] ?? "-")
                          )}
                        </td>
                      ))}
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          )}
        </CardBody>
      </Card>
      
      {/* Centered Pagination */}
      <div className="mt-6 flex justify-center gap-2">
        <Button size="sm" variant="text" onClick={() => setCurrentPage(1)} disabled={currentPage === 1}>First</Button>
        <Button size="sm" variant="text" onClick={() => setCurrentPage((p) => Math.max(1, p - 1))} disabled={currentPage === 1}>Prev</Button>
        <div className="flex items-center px-4 border rounded-md bg-white text-sm font-bold">{currentPage}</div>
        <Button size="sm" variant="text" onClick={() => setCurrentPage((p) => Math.min(totalPages, p + 1))} disabled={currentPage === totalPages}>Next</Button>
        <Button size="sm" variant="text" onClick={() => setCurrentPage(totalPages)} disabled={currentPage === totalPages}>Last</Button>
      </div>
    </div>
  );
};

export default ZomatoData;