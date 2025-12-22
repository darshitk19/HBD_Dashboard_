import api from "./Api";

export const downloadCSV = async (type) => {
  try {
    const response = await api.get(
      `/items/${type}/csv`,
      {
        responseType: "blob", //  binary file download
      }
    );

    // Blob se download trigger 
    const url = window.URL.createObjectURL(new Blob([response.data]));
    const link = document.createElement("a");
    link.href = url;
    link.setAttribute("download", `${type}_data.csv`); // filename set
    document.body.appendChild(link);
    link.click();
    link.remove();
  } catch (error) {
    console.error("CSV download error:", error);
  }
};

