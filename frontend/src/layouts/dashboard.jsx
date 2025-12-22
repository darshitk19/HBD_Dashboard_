import { Routes, Route } from "react-router-dom";
import { Cog6ToothIcon } from "@heroicons/react/24/solid";
import { IconButton } from "@material-tailwind/react";
import  Footer  from "../widgets/layout/footer";
import DashboardNavbar from "../widgets/layout/dashboard-navbar";
import Sidenav from "../widgets/layout/sidenav";
import Configurator from "../widgets/layout/configurator";

import routes from "../routes";
import { useMaterialTailwindController } from "../context/index";
import { setOpenConfigurator } from "../context/index";
import React from "react";

export function Dashboard() {
  const [controller, dispatch] = useMaterialTailwindController();
  const { sidenavType } = controller;

  const flattenRoutes = (pages) => {
  let routes = [];
  for (const page of pages) {
   
    if ( page.path && page.element) {
      routes.push({ path: page.path, element: page.element });
    }
    if (page.children) {
      routes = routes.concat(flattenRoutes(page.children));
    }
  }
  return routes;
};



  return (
    <div className="min-h-screen bg-blue-gray-50/50">
      <Sidenav 
        routes={
          routes.map((r) => ({...r,
            pages: r.pages.filter((p) => !p.hidden),}))}
        brandImg={
          sidenavType === "dark" ? "/img/logo-ct.png" : "/img/logo-ct-dark.png"
        }
      />
      <div className="p-4 xl:ml-80">
        <DashboardNavbar />
        <Configurator />
        <IconButton
          size="lg"
          color="white"
          className="fixed bottom-8 right-8 z-40 rounded-full shadow-blue-gray-900/10"
          ripple={false}
          onClick={() => setOpenConfigurator(dispatch, true)}
        >
          <Cog6ToothIcon className="h-5 w-5" />
        </IconButton>
        <Routes>
          {routes
            .filter((r) => r.layout === "dashboard")
            .flatMap((r) => flattenRoutes(r.pages))
            .map(({ path, element }) => (
              <Route key={path} path={path} element={element} />
            ))}
        </Routes>

        <div className="text-blue-gray-600">
          <Footer />
        </div>
      </div>
    </div>
  );
}

Dashboard.displayName = "/src/layout/dashboard.jsx";

export default Dashboard;
