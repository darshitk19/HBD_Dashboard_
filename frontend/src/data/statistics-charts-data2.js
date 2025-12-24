// import { chartsConfig } from "@/configs";
import { chartsConfig } from "../configs/charts-config";

const dataScrapped = {
  type: "bar",
  height: 220,
  series: [
    {
      name: "Scraped Data",
      data: [700, 900, 1800],
    },
  ],
  options: {
    ...chartsConfig,
    colors: "#388e3c",
    plotOptions: {
      bar: {
        columnWidth: "16%",
        borderRadius: 5,
      },
    },
    xaxis: {
      ...chartsConfig.xaxis,
      categories: ["D-Mart","Amazon", "Google Map"],
    },
  },
};

const scrappingTrend = {
  type: "line",
  height: 220,
  series: [
    {
      name: "Scraped Data",
      data: [700, 900, 1800],
    },
  ],
  options: {
    ...chartsConfig,
    colors: ["#0288d1"],
    stroke: {
      lineCap: "round",
    },
    markers: {
      size: 5,
    },
    xaxis: {
      ...chartsConfig.xaxis,
      categories: ["D-Mart","Amazon", "Google Map"],
    },
  },
};

const categoriesData = {
  type: "pie",
  height: 220,
  series: [700, 300], // Product Listing, Others
  options: {
    labels: ["Product Listing", "Others"],
    colors: ["#008FFB", "#00E396"],
    legend: {
      position: "bottom",
    },
    responsive: [
      {
        breakpoint: 480,
        options: {
          chart: {
            width: 300,
          },
          legend: {
            position: "bottom",
          },
        },
      },
    ],
  },
};


export const statisticsChartsData2 = [
  {
    color: "white",
    title: "Source-wise Data Scraped",
    description: "Last Campaign Performance",
    footer: "campaign sent 2 days ago",
    chart: dataScrapped,
  },
  {
    color: "white",
    title: "Scraping Trend",
    description: "Last Campaign Performance",
    footer: "just updated",
    chart: scrappingTrend,
  },
  {
    color: "white",
    title: "Scraping Trend",
    description: "Last Campaign Performance",
    footer: "just updated",
    chart: categoriesData,
  },
];

export default statisticsChartsData2;