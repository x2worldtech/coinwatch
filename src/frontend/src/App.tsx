import { Skeleton } from "@/components/ui/skeleton";
import {
  Outlet,
  RouterProvider,
  createRootRoute,
  createRoute,
  createRouter,
} from "@tanstack/react-router";
import { Suspense, lazy } from "react";

const MarketPage = lazy(() => import("@/pages/MarketPage"));

const rootRoute = createRootRoute({
  component: () => <Outlet />,
});

const indexRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/",
  component: () => (
    <Suspense
      fallback={
        <div className="min-h-screen bg-background flex flex-col gap-3 p-6">
          {[
            "sk-1",
            "sk-2",
            "sk-3",
            "sk-4",
            "sk-5",
            "sk-6",
            "sk-7",
            "sk-8",
            "sk-9",
            "sk-10",
            "sk-11",
            "sk-12",
          ].map((id) => (
            <Skeleton key={id} className="h-14 w-full rounded-lg" />
          ))}
        </div>
      }
    >
      <MarketPage />
    </Suspense>
  ),
});

const routeTree = rootRoute.addChildren([indexRoute]);

const router = createRouter({ routeTree });

declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}

export default function App() {
  return <RouterProvider router={router} />;
}
