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
import { amazonProductData } from "../../data/amazonJSON";
import * as XLSX from "xlsx/dist/xlsx.full.min.js";

const defaultColumns = [
  { key: "ASIN", label: "ASIN", width: 140 },
  { key: "title", label: "Product Title", width: 350 },
  { key: "price", label: "Price", width: 100 },
  { key: "rating", label: "Rating", width: 90 },
  { key: "review_count", label: "Reviews", width: 110 },
  { key: "best_seller_rank", label: "BSR", width: 150 },
  { key: "brand", label: "Brand", width: 130 },
  { key: "availability", label: "Stock Status", width: 120 },
  { key: "category", label: "Main Category", width: 180 },
  { key: "url", label: "Amazon Link", width: 250 },
  { key: "image_url", label: "Image Link", width: 200 },
  { key: "manufacturer", label: "Manufacturer", width: 180 },
];

const convertToCSV = (arr) => {
  if (!arr.length) return "";
  const headers = Object.keys(arr[0]);
  const rows = arr.map((r) =>
    headers.map((h) => `"${String(r[h] ?? "").replace(/"/g, "'")}"`).join(",")
  );
  return [headers.join(","), ...rows].join("\n");
};

const BigBasketData = () => {
  const [loading, setLoading] = useState(true);
  const [fullData, setFullData] = useState([]);
  const [pageData, setPageData] = useState([]);
  const [currentPage, setCurrentPage] = useState(1);
  const [total, setTotal] = useState(0);
  const limit = 10;

  const [search, setSearch] = useState("");
  const [categoryFilter, setCategoryFilter] = useState("");

  const [sortField, setSortField] = useState(null);
  const [sortOrder, setSortOrder] = useState("asc");
  const [columns, setColumns] = useState(defaultColumns);

  useEffect(() => {
    setLoading(true);
    // Simulate API fetch from your JSON data
    setTimeout(() => {
      const data = amazonProductData || [];
      setFullData(data);
      setTotal(data.length);
      setLoading(false);
    }, 500);
  }, []);

  const filteredData = useMemo(() => {
    let data = [...fullData];
    if (search) {
      data = data.filter((x) =>
        (x.title || "").toLowerCase().includes(search.toLowerCase()) ||
        (x.ASIN || "").toLowerCase().includes(search.toLowerCase())
      );
    }
    if (categoryFilter) {
      data = data.filter((x) =>
        (x.category || "").toLowerCase().includes(categoryFilter.toLowerCase())
      );
    }
    return data;
  }, [fullData, search, categoryFilter]);

  const sortedData = useMemo(() => {
    if (!sortField) return filteredData;
    return [...filteredData].sort((a, b) => {
      const A = a[sortField] ?? "";
      const B = b[sortField] ?? "";
      
      // Handle numeric sorting for price/rating/reviews
      if (!isNaN(A) && !isNaN(B) && A !== "" && B !== "") {
        return sortOrder === "asc" ? A - B : B - A;
      }
      
      const strA = String(A).toLowerCase();
      const strB = String(B).toLowerCase();
      if (strA === strB) return 0;
      return sortOrder === "asc" ? (strA > strB ? 1 : -1) : (strA < strB ? 1 : -1);
    });
  }, [filteredData, sortField, sortOrder]);

  useEffect(() => {
    const start = (currentPage - 1) * limit;
    setPageData(sortedData.slice(start, start + limit));
    setTotal(sortedData.length);
  }, [sortedData, currentPage]);

  const totalPages = Math.max(1, Math.ceil(total / limit));

  const toggleSort = (field) => {
    if (sortField === field) setSortOrder((o) => (o === "asc" ? "desc" : "asc"));
    else {
      setSortField(field);
      setSortOrder("asc");
    }
  };

  const downloadExcel = (currentOnly = false) => {
    const arr = currentOnly ? pageData : fullData;
    const ws = XLSX.utils.json_to_sheet(arr);
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, "Amazon_Data");
    XLSX.writeFile(wb, currentOnly ? "amazon_page.xlsx" : "amazon_all.xlsx");
  };

  const startResize = (colKey, e) => {
    e.preventDefault();
    const startX = e.clientX;
    const col = columns.find((c) => c.key === colKey);
    const startWidth = col.width;

    const onMouseMove = (ev) => {
      const delta = ev.clientX - startX;
      setColumns((cols) =>
        cols.map((c) => (c.key === colKey ? { ...c, width: Math.max(80, startWidth + delta) } : c))
      );
    };

    const stop = () => {
      document.removeEventListener("mousemove", onMouseMove);
      document.removeEventListener("mouseup", stop);
    };
    document.addEventListener("mousemove", onMouseMove);
    document.addEventListener("mouseup", stop);
  };

  return (
    <div className="min-h-screen mt-8 mb-12 px-4">
      <div className="flex flex-col md:flex-row justify-between items-start md:items-center mb-6 gap-4">
        <div>
          <Typography variant="h4" color="blue-gray">Amazon Inventory</Typography>
          <Typography variant="small" className="text-gray-600">Managing {total} scraped products</Typography>
        </div>

        <div className="flex items-center gap-2">
          <Button variant="outlined" size="sm" onClick={() => downloadExcel(false)} className="flex items-center gap-2">
            Export All (Excel)
          </Button>
          <Button variant="gradient" color="blue" size="sm" onClick={() => downloadExcel(true)}>
            Export Page
          </Button>
        </div>
      </div>

      <Card className="h-full w-full shadow-sm border border-blue-gray-50">
        <CardHeader floated={false} shadow={false} className="rounded-none p-4">
          <div className="flex flex-col md:flex-row items-center justify-between gap-4">
            <div className="flex w-full md:w-max gap-2">
              <Input
                label="Search ASIN or Title"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                icon={<MagnifyingGlassIcon className="h-5 w-5" />}
              />
              <Input
                label="Filter Category"
                value={categoryFilter}
                onChange={(e) => setCategoryFilter(e.target.value)}
              />
            </div>
            <div className="flex items-center gap-2">
                <Typography variant="small" className="font-bold">
                    Page {currentPage} of {totalPages}
                </Typography>
            </div>
          </div>
        </CardHeader>

        <CardBody className="p-0 overflow-x-auto">
          {loading ? (
            <div className="flex justify-center py-20"><Spinner className="h-12 w-12" /></div>
          ) : (
            <table className="w-full table-fixed border-collapse min-w-[1200px]">
              <thead className="bg-blue-gray-50/50 sticky top-0 z-10">
                <tr>
                  {columns.map((col) => (
                    <th
                      key={col.key}
                      style={{ width: col.width }}
                      className="border-y border-blue-gray-100 p-4 text-left relative"
                    >
                      <div className="flex items-center justify-between">
                        <div className="flex items-center gap-2 cursor-pointer" onClick={() => toggleSort(col.key)}>
                          <Typography variant="small" color="blue-gray" className="font-bold leading-none opacity-70">
                            {col.label}
                          </Typography>
                          {sortField === col.key ? (
                            sortOrder === "asc" ? <ChevronUpDownIcon className="h-4 w-4" /> : <ChevronDownIcon className="h-4 w-4" />
                          ) : <ChevronUpDownIcon className="h-4 w-4 opacity-30" />}
                        </div>
                        <div onMouseDown={(e) => startResize(col.key, e)} className="absolute right-0 top-0 h-full w-1 cursor-col-resize hover:bg-blue-500/30" />
                      </div>
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {pageData.length === 0 ? (
                  <tr><td colSpan={columns.length} className="text-center p-10 text-gray-500">No Amazon data found.</td></tr>
                ) : (
                  pageData.map((row, index) => (
                    <tr key={index} className="even:bg-blue-gray-50/50 hover:bg-blue-50/50 transition-colors">
                      {columns.map((col) => (
                        <td key={col.key} className="p-4 text-sm border-b border-blue-gray-50 truncate">
                          {col.key === "url" ? (
                            <a href={row[col.key]} target="_blank" rel="noreferrer" className="text-blue-600 font-medium hover:underline">
                              View Link
                            </a>
                          ) : col.key === "price" ? (
                            <span className="font-bold text-green-700">₹{row[col.key] || "0.00"}</span>
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

      {/* Pagination Controls */}
      <div className="flex items-center justify-between border-t border-blue-gray-50 p-4">
        <Typography variant="small" color="blue-gray" className="font-normal">
          Showing {pageData.length} of {total} products
        </Typography>
        <div className="flex gap-2">
          <Button variant="outlined" size="sm" onClick={() => setCurrentPage(1)} disabled={currentPage === 1}>First</Button>
          <Button variant="outlined" size="sm" onClick={() => setCurrentPage((p) => Math.max(1, p - 1))} disabled={currentPage === 1}>Prev</Button>
          <Button variant="outlined" size="sm" onClick={() => setCurrentPage((p) => Math.min(totalPages, p + 1))} disabled={currentPage === totalPages}>Next</Button>
        </div>
      </div>
    </div>
  );
};

export default BigBasketData;