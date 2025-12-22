import React, { useEffect, useState } from "react";
import {
  Card,
  CardBody,
  Typography,
  List,
  ListItem,
  ListItemPrefix,
  Spinner,
  Input,
} from "@material-tailwind/react";
import { MapPinIcon, FolderIcon, MagnifyingGlassIcon } from "@heroicons/react/24/solid";
import api from "../utils/Api";

export default function ProductDataReport() {
  const [areas, setAreas] = useState([]);
  const [categories, setCategories] = useState([]);
  const [loading, setLoading] = useState(true);

  const [areaSearch, setAreaSearch] = useState("");
  const [categorySearch, setCategorySearch] = useState("");

  // Simulate API fetch
  useEffect(() => {
    setTimeout(() => {
      setAreas(["New York", "London", "Paris", "Mumbai", "Tokyo", "Sydney"]);
      setCategories([
        "Restaurants",
        "Shops",
        "Events",
        "Hotels",
        "Hospitals",
        "Schools",
      ]);
      setLoading(false);
    }, 1000);
  }, []);

  // Filtered lists
  const filteredAreas = areas.filter((area) =>
    area.toLowerCase().includes(areaSearch.toLowerCase())
  );
  const filteredCategories = categories.filter((cat) =>
    cat.toLowerCase().includes(categorySearch.toLowerCase())
  );

  return (<>
    <Typography variant="h4" >
      Service Data Report
    </Typography>
    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 p-6">
      {/* Total Area Count */}
      <Card className="shadow-md">
        <CardBody>
          <Typography variant="h5" color="blue-gray" className="mb-2">
            Total Areas
          </Typography>
          {loading ? (
            <Spinner className="h-6 w-6" />
          ) : (
            <Typography variant="h3" color="blue">
              {areas.length}
            </Typography>
          )}
        </CardBody>
      </Card>

      {/* Total Category Count */}
      <Card className="shadow-md">
        <CardBody>
          <Typography variant="h5" color="blue-gray" className="mb-2">
            Total Categories
          </Typography>
          {loading ? (
            <Spinner className="h-6 w-6" />
          ) : (
            <Typography variant="h3" color="green">
              {categories.length}
            </Typography>
          )}
        </CardBody>
      </Card>

      {/* List of Areas with Search */}
      <Card className="shadow-md lg:col-span-1">
        <CardBody>
          <Typography variant="h6" color="blue-gray" className="mb-4">
            Areas
          </Typography>

          {/* Search Input */}
          <div className="mb-4">
            <Input
              label="Search Areas"
              icon={<MagnifyingGlassIcon className="h-5 w-5" />}
              value={areaSearch}
              onChange={(e) => setAreaSearch(e.target.value)}
            />
          </div>

          {loading ? (
            <Spinner className="h-6 w-6" />
          ) : (
            <List>
              {filteredAreas.length > 0 ? (
                filteredAreas.map((area, i) => (
                  <ListItem key={i}>
                    <ListItemPrefix>
                      <MapPinIcon className="h-5 w-5 text-blue-500" />
                    </ListItemPrefix>
                    {area}
                  </ListItem>
                ))
              ) : (
                <Typography color="red" className="text-sm">
                  No areas found
                </Typography>
              )}
            </List>
          )}
        </CardBody>
      </Card>

      {/* List of Categories with Search */}
      <Card className="shadow-md lg:col-span-1">
        <CardBody>
          <Typography variant="h6" color="blue-gray" className="mb-4">
            Categories
          </Typography>

          {/* Search Input */}
          <div className="mb-4">
            <Input
              label="Search Categories"
              icon={<MagnifyingGlassIcon className="h-5 w-5" />}
              value={categorySearch}
              onChange={(e) => setCategorySearch(e.target.value)}
            />
          </div>

          {loading ? (
            <Spinner className="h-6 w-6" />
          ) : (
            <List>
              {filteredCategories.length > 0 ? (
                filteredCategories.map((cat, i) => (
                  <ListItem key={i}>
                    <ListItemPrefix>
                      <FolderIcon className="h-5 w-5 text-green-500" />
                    </ListItemPrefix>
                    {cat}
                  </ListItem>
                ))
              ) : (
                <Typography color="red" className="text-sm">
                  No categories found
                </Typography>
              )}
            </List>
          )}
        </CardBody>
      </Card>
    </div>
  </>);
}
