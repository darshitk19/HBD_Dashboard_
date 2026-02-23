import React, { useEffect, useState, useMemo } from "react";
import api from "@/utils/Api";
import {
  Button, Card, CardBody, CardHeader, Typography, Input, Spinner,
} from "@material-tailwind/react";
import {
  ArrowDownTrayIcon, MagnifyingGlassIcon, ChevronUpDownIcon, ChevronDownIcon,
} from "@heroicons/react/24/solid";
import * as XLSX from "xlsx/dist/xlsx.full.min.js";

const googleMapColumns = [
  { key: "name", label: "Business Name", width: 250 },
  { key: "address", label: "Address", width: 350 },
  { key: "phone_number", label: "Contact No", width: 150 },
  { key: "category", label: "Category", width: 180 },
  { key: "city", label: "City", width: 120 },
  { key: "rating", label: "Rating", width: 100 },
];

const GoogleMapData = () => {
  const [loading, setLoading] = useState(true);
  const [pageData, setPageData] = useState([]);
  const [currentPage, setCurrentPage] = useState(1);
  const [totalPages, setTotalPages] = useState(1);
  const [totalRecords, setTotalRecords] = useState(0);
  const limit = 10;

  const [search, setSearch] = useState("");
  const [citySearch, setCitySearch] = useState("");
  const [error, setError] = useState(null);

  useEffect(() => {
    let isMounted = true;
    const fetchData = async () => {
      setLoading(true);
      setError(null);
      try {
        const queryParams = new URLSearchParams({
          page: currentPage,
          limit: limit,
          search: search,
          city: citySearch,
        });

        const response = await api.get(`/google-listings?${queryParams}`);
        if (isMounted) {
          const result = response.data;
          setPageData(result.data || []);
          setTotalPages(result.total_pages || 1);
          setTotalRecords(result.total_count || 0);
        }
      } catch (err) {
        console.error("Fetch Error:", err);
        if (isMounted) {
          if (err.code === "ERR_NETWORK") {
            setError("Backend offline. Check connection.");
          } else {
            setError("Failed to fetch Google Map data.");
          }
        }
      } finally {
        if (isMounted) setLoading(false);
      }
    };
    fetchData();
    return () => { isMounted = false; };
  }, [currentPage, limit, search, citySearch]);

  const exportToExcel = () => {
    if (!pageData.length) return;
    const ws = XLSX.utils.json_to_sheet(pageData);
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, "GoogleMap_Data");
    XLSX.writeFile(wb, `GoogleMap_Data_Page_${currentPage}.xlsx`);
  };

  return (
    <div className="min-h-screen mt-8 mb-12 px-4 rounded bg-white text-black">
      {/* Header Section */}
      <div className="flex justify-between items-end mb-6">
        <div>
          <Typography variant="h4" className="font-bold text-blue-gray-900">
            Google Maps Listing Master
          </Typography>
          <Typography variant="small" className="font-medium text-gray-500">
            {error ? (
              <span className="text-red-500 font-bold">{error}</span>
            ) : (
              `Displaying verified Google Map records (${totalRecords} total)`
            )}
          </Typography>
        </div>
        <div className="flex gap-2">
          <Button
            variant="gradient"
            color="green"
            size="sm"
            className="flex items-center gap-2"
            onClick={exportToExcel}
          >
            <ArrowDownTrayIcon className="h-4 w-4" /> Export Page
          </Button>
        </div>
      </div>

      <Card className="h-full w-full border border-blue-gray-100">
        <CardHeader floated={false} shadow={false} className="rounded-none p-4 bg-blue-gray-50/50">
          <div className="flex flex-wrap items-center justify-between gap-y-4">
            <div className="flex w-full shrink-0 gap-2 md:w-max">
              <div className="w-72">
                <Input
                  label="Search Business Name"
                  value={search}
                  onChange={(e) => {
                    setSearch(e.target.value);
                    setCurrentPage(1);
                  }}
                />
              </div>
              <div className="w-48">
                <Input
                  label="Filter by City"
                  value={citySearch}
                  onChange={(e) => {
                    setCitySearch(e.target.value);
                    setCurrentPage(1);
                  }}
                />
              </div>
            </div>

            <div className="flex items-center gap-4">
              <Typography variant="small" className="font-bold text-blue-gray-700">
                Page {currentPage} of {totalPages}
              </Typography>
              <div className="flex gap-2">
                <Button
                  variant="outlined"
                  size="sm"
                  disabled={currentPage === 1 || loading}
                  onClick={() => setCurrentPage(p => p - 1)}
                >
                  Previous
                </Button>
                <Button
                  variant="outlined"
                  size="sm"
                  disabled={currentPage === totalPages || loading}
                  onClick={() => setCurrentPage(p => p + 1)}
                >
                  Next
                </Button>
              </div>
            </div>
          </div>
        </CardHeader>

        <CardBody className="p-0 overflow-x-auto">
          {loading ? (
            <div className="flex flex-col justify-center py-24 items-center gap-4">
              <Spinner className="h-10 w-10 text-blue-500" />
              <Typography className="animate-pulse font-medium text-gray-600">
                Loading Google Map Data...
              </Typography>
            </div>
          ) : (
            <table className="w-full table-fixed border-collapse min-w-[1500px]">
              <thead className="sticky top-0 z-20 border-b bg-gray-200">
                <tr>
                  {googleMapColumns.map((col) => (
                    <th key={col.key} style={{ width: col.width }} className="px-3 py-2 text-left">
                      <div className="flex items-center gap-2">
                        <span className="capitalize text-sm font-semibold">{col.label}</span>
                      </div>
                    </th>
                  ))}
                </tr>
              </thead>

              <tbody>
                {pageData.length === 0 ? (
                  <tr>
                    <td colSpan={googleMapColumns.length} className="p-20 text-center">
                      <Typography variant="h6" color="blue-gray" className="opacity-40 italic">
                        {error || "No Google Map records found."}
                      </Typography>
                    </td>
                  </tr>
                ) : (
                  pageData.map((row, idx) => (
                    <tr key={idx} className="border-b hover:bg-gray-50">
                      {googleMapColumns.map((col) => (
                        <td key={col.key} style={{ width: col.width }} className="px-3 py-3 break-words text-sm">
                          {String(row[col.key] ?? "-")}
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
    </div>
  );
};

export default GoogleMapData;
