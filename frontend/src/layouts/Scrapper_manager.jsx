import React, { useState, useEffect, useRef } from "react";
import {
  Card,
  CardHeader,
  CardBody,
  Typography,
  Button,
  Chip,
  Progress,
  IconButton,
  Dialog,
  DialogHeader,
  DialogBody,
  DialogFooter,
  Input,
  Select,
  Option,
  Alert,
  Tooltip,
} from "@material-tailwind/react";
import {
  PlusIcon,
  TrashIcon,
  StopIcon,
  ExclamationTriangleIcon,
  MagnifyingGlassIcon,
  MapIcon,
  ShoppingBagIcon,
} from "@heroicons/react/24/solid";
import api from "../utils/Api";

export function ScraperManager() {
  const [tasks, setTasks] = useState([]);
  const [open, setOpen] = useState(false);
  const [globalError, setGlobalError] = useState(null);
  const [stopCooldown, setStopCooldown] = useState({});

  const [platform, setPlatform] = useState("Google Maps");
  const [category, setCategory] = useState("");
  const [city, setCity] = useState("");
  const [state, setState] = useState("");
  const [searchTerm, setSearchTerm] = useState("");

  const pollInterval = useRef(null);

  const fetchTasks = async () => {
    try {
      const response = await api.get("/api/tasks");
      setTasks(response.data);
    } catch (err) {
      console.error("Polling error:", err);
    }
  };

  useEffect(() => {
    fetchTasks();
    pollInterval.current = setInterval(fetchTasks, 3000);
    return () => clearInterval(pollInterval.current);
  }, []);

  const handleDelete = async (id) => {
    if (!window.confirm("Are you sure you want to delete this task?")) return;

    try {
      await api.delete(`/api/tasks/${id}`);
      setTasks((prev) => prev.filter((task) => task.id !== id));
    } catch (err) {
      alert("Failed to delete task from database.");
      console.error(err);
    }
  };

  const handleOpen = () => {
    setOpen(!open);
    setGlobalError(null);
    if (!open) {
      setCategory("");
      setCity("");
      setState("");
      setSearchTerm("");
    }
  };

  const handleStartScraper = async () => {
    let payload = { platform };
    let endpoint = "";

    if (platform === "Google Maps") {
      if (!category.trim() || !city.trim() || !state.trim()) {
        return alert(
          "STOP! Category, City, and State are ALL mandatory for Google Maps."
        );
      }
      endpoint = "/api/scrape";
      payload = { ...payload, category, city, state };
    } else {
      if (!searchTerm.trim()) {
        return alert("STOP! Please enter a product name for Amazon.");
      }
      endpoint = "/api/scrape_amazon";
      payload = { ...payload, search_term: searchTerm, pages: 1 };
    }

    try {
      const response = await api.post(endpoint, payload);
      if (response.status === 202) {
        setOpen(false);
        setCategory("");
        setCity("");
        setState("");
        setSearchTerm("");
        fetchTasks();
      }
    } catch (err) {
      console.error(err);
      setGlobalError("Failed to start scraper. Check backend connection.");
    }
  };

  const handleStop = async (taskId) => {
    setStopCooldown((prev) => ({ ...prev, [taskId]: true }));
    try {
      await api.post("/api/stop", { task_id: taskId });
      fetchTasks();
    } catch (err) {
      alert("Error sending stop signal.");
    }
    setTimeout(() => {
      setStopCooldown((prev) => ({ ...prev, [taskId]: false }));
    }, 10000);
  };

  return (
    <div className="relative h-screen w-full overflow-hidden">
      {/* COMING SOON OVERLAY */}
      <div className="absolute inset-0 z-[9999] flex flex-col items-center justify-center bg-gray-900/60 backdrop-blur-md">
        <div className="rounded-xl border border-white/10 bg-black/40 p-10 text-center shadow-2xl backdrop-blur-sm">
          <Typography
            variant="h1"
            color="white"
            className="mb-2 font-black uppercase tracking-widest text-4xl lg:text-6xl"
          >
            Coming Soon
          </Typography>
          <Typography
            variant="lead"
            color="white"
            className="font-normal opacity-80"
          >
            This module is currently under construction.
          </Typography>
        </div>
      </div>

      {/* DISABLED CONTENT BEHIND OVERLAY */}
      <div className="pointer-events-none select-none h-full overflow-y-auto opacity-50">
        <div className="mt-12 flex w-full flex-col gap-6 px-4 bg-slate-50/50 min-h-screen pb-10">
          {globalError && (
            <Alert
              color="red"
              variant="gradient"
              icon={<ExclamationTriangleIcon className="h-6 w-6" />}
              onClose={() => setGlobalError(null)}
              className="shadow-md"
            >
              <Typography variant="h6" color="white">
                System Alert
              </Typography>
              <Typography variant="small" color="white" className="opacity-80">
                {globalError}
              </Typography>
            </Alert>
          )}

          <Card className="relative overflow-hidden border border-gray-200 bg-gradient-to-br from-white to-gray-500/30 shadow-md transition-all">
            <CardHeader
              floated={false}
              shadow={false}
              className="m-0 p-6 flex justify-between items-center bg-white/50 border-b border-gray-200"
            >
              <div>
                <Typography
                  variant="h5"
                  color="blue-gray"
                  className="font-black tracking-tight uppercase"
                >
                  Scraper Control Terminal
                </Typography>
                <Typography className="font-normal text-gray-500 text-sm italic">
                  Real-time synchronization active
                </Typography>
              </div>
              <Button
                className="flex items-center gap-3 bg-gray-900 shadow-sm hover:shadow-lg transition-all active:scale-95"
                size="sm"
                onClick={handleOpen}
              >
                <PlusIcon strokeWidth={3} className="h-4 w-4" /> Add Scraper
              </Button>
            </CardHeader>

            <CardBody className="px-0 pt-0 pb-2 overflow-x-auto">
              {tasks.length === 0 ? (
                <div className="flex flex-col items-center justify-center py-20 opacity-40">
                  <MagnifyingGlassIcon className="h-12 w-12 mb-2" />
                  <Typography variant="h6">No history found</Typography>
                </div>
              ) : (
                <table className="w-full min-w-[640px] table-auto">
                  <thead>
                    <tr>
                      {[
                        "Platform",
                        "Target Query",
                        "Status",
                        "Real-Time Progress",
                        "Actions",
                      ].map((el) => (
                        <th
                          key={el}
                          className="border-b border-blue-gray-50 py-4 px-6 text-left"
                        >
                          <Typography className="text-[10px] font-black uppercase text-blue-gray-400 tracking-widest">
                            {el}
                          </Typography>
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {tasks.map((task) => (
                      <tr
                        key={task.id}
                        className="hover:bg-white/60 transition-colors border-b border-gray-100/50"
                      >
                        <td className="py-4 px-6">
                          <div className="flex items-center gap-2">
                            {task.platform === "Amazon" ? (
                              <ShoppingBagIcon className="h-4 w-4 text-orange-700" />
                            ) : (
                              <MapIcon className="h-4 w-4 text-blue-700" />
                            )}
                            <Typography
                              variant="small"
                              className="font-bold text-blue-gray-800"
                            >
                              {task.platform}
                            </Typography>
                          </div>
                        </td>
                        <td className="py-4 px-6">
                          <Typography
                            variant="small"
                            className="font-medium text-gray-600 truncate max-w-[200px]"
                          >
                            {task.query || task.search_query || "N/A"}
                          </Typography>
                        </td>
                        <td className="py-4 px-6">
                          <Chip
                            size="sm"
                            variant="ghost"
                            value={task.status}
                            className="rounded-full font-bold px-3 text-center"
                            color={
                              task.status === "COMPLETED"
                                ? "green"
                                : task.status === "RUNNING"
                                ? "blue"
                                : task.status === "STOPPED"
                                ? "orange"
                                : "red"
                            }
                          />
                        </td>
                        <td className="py-4 px-6 w-72">
                          <div className="flex flex-col gap-1">
                            <Typography className="text-[10px] font-black text-blue-gray-700">
                              {task.progress}%
                            </Typography>
                            <Progress
                              value={task.progress}
                              size="sm"
                              color={
                                task.status === "COMPLETED" ? "green" : "blue"
                              }
                              className="h-1.5 bg-gray-200"
                            />
                          </div>
                        </td>
                        <td className="py-4 px-6 flex gap-2">
                          {task.status === "RUNNING" && (
                            <Tooltip
                              content={
                                stopCooldown[task.id]
                                  ? "Halt signal processing..."
                                  : "Immediate Stop"
                              }
                            >
                              <IconButton
                                variant="text"
                                color="orange"
                                className="bg-orange-50 hover:bg-orange-100"
                                disabled={stopCooldown[task.id]}
                                onClick={() => handleStop(task.id)}
                              >
                                <StopIcon className="h-5 w-5" />
                              </IconButton>
                            </Tooltip>
                          )}
                          <IconButton
                            variant="text"
                            color="red"
                            className="bg-red-50 hover:bg-red-100"
                            onClick={() => handleDelete(task.id)}
                          >
                            <TrashIcon className="h-5 w-5" />
                          </IconButton>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </CardBody>
          </Card>

          <Dialog
            open={open}
            handler={handleOpen}
            size="xs"
            className="bg-white rounded-2xl shadow-2xl"
          >
            <DialogHeader className="flex flex-col items-start gap-1">
              <Typography
                variant="h4"
                color="blue-gray"
                className="font-black uppercase tracking-tighter"
              >
                Initialize Scraper
              </Typography>
              <Typography className="font-normal text-gray-500 text-xs">
                Fields marked with * are required for execution
              </Typography>
            </DialogHeader>
            <DialogBody className="flex flex-col gap-6 py-4">
              <Select
                label="Select Scraper Platform"
                value={platform}
                onChange={(val) => setPlatform(val)}
              >
                <Option value="Google Maps">Google Maps Deep Scraper</Option>
                <Option value="Amazon">Amazon Product Scraper</Option>
              </Select>

              {platform === "Google Maps" ? (
                <div className="flex flex-col gap-4 animate-in slide-in-from-top-2 duration-300">
                  <Input
                    label="Business Category*"
                    value={category}
                    onChange={(e) => setCategory(e.target.value)}
                  />
                  <Input
                    label="City*"
                    value={city}
                    onChange={(e) => setCity(e.target.value)}
                  />
                  <Input
                    label="State*"
                    value={state}
                    onChange={(e) => setState(e.target.value)}
                  />
                </div>
              ) : (
                <div className="animate-in slide-in-from-top-2 duration-300">
                  <Input
                    label="Product Search Term*"
                    value={searchTerm}
                    onChange={(e) => setSearchTerm(e.target.value)}
                  />
                </div>
              )}
            </DialogBody>
            <DialogFooter className="gap-2 border-t border-gray-100">
              <Button
                variant="text"
                color="red"
                onClick={handleOpen}
                className="normal-case font-bold"
              >
                Cancel
              </Button>
              <Button
                variant="gradient"
                color="gray"
                onClick={handleStartScraper}
                className="bg-gray-900 normal-case font-black shadow-none hover:shadow-lg transition-all"
              >
                Execute Pipeline
              </Button>
            </DialogFooter>
          </Dialog>
        </div>
      </div>
    </div>
  );
}

export default ScraperManager;