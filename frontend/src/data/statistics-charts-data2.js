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
];

export default statisticsChartsData2;