import axios from "axios";
import React, { useState } from "react";
import api from "../../utils/Api";

const GoogleMapScrapper = () => {
  const [category, setCategory] = useState("");
  const [area, setArea] = useState("");
  const [state, setState] = useState("");

  const handleSearch = async () => {
    const searchQuery = `${category},${area},${state}`;
    console.log(searchQuery); // Example: "restaurant, downtown, New York"

    // Uncomment if you want to send to backend
    try {
     const response = await api.post("/api/scrape", {
      queries: [searchQuery],   // 👈 backend expects an array named "queries"
    });
    console.log(response.data);
    } catch (error) {
      console.error("Error during search:", error);
    }

     // Clear inputs after search
    setCategory("");
    setArea("");
    setState("");
  };

  return (
    <div style={{ padding: "20px" }}>
      {/* Category */}
      <div style={{ marginBottom: "10px" }}>
        <label style={{ display: "block", marginBottom: "5px", fontWeight: "bold" }}>
          Category
        </label>
        <input
          type="text"
          placeholder="e.g. Restaurant"
          value={category}
          onChange={(e) => setCategory(e.target.value)}
          style={{
            width: "15rem",
            padding: "8px",
            border: "1px solid #ccc",
            borderRadius: "4px",
          }}
        />
      </div>

      {/* Area */}
      <div style={{ marginBottom: "10px" }}>
        <label style={{ display: "block", marginBottom: "5px", fontWeight: "bold" }}>
          Area
        </label>
        <input
          type="text"
          placeholder="e.g. Ahmedabad"
          value={area}
          onChange={(e) => setArea(e.target.value)}
          style={{
            width: "15rem",
            padding: "8px",
            border: "1px solid #ccc",
            borderRadius: "4px",
          }}
        />
      </div>

      {/* State */}
      <div style={{ marginBottom: "10px" }}>
        <label style={{ display: "block", marginBottom: "5px", fontWeight: "bold" }}>
          State
        </label>
        <input
          type="text"
          placeholder="e.g. Gujarat"
          value={state}
          onChange={(e) => setState(e.target.value)}
          style={{
            width: "15rem",
            padding: "8px",
            border: "1px solid #ccc",
            borderRadius: "4px",
          }}
        />
      </div>

      {/* Search Button */}
      <button
        onClick={handleSearch}
        style={{
          padding: "8px 16px",
          backgroundColor: "#007bff",
          color: "#fff",
          border: "none",
          borderRadius: "4px",
          cursor: "pointer",
        }}
      >
        Search
      </button>
    </div>
  );
};

export default GoogleMapScrapper;
