import {
  HeadContent,
  Link,
  Outlet,
  Scripts,
  createRootRoute,
} from "@tanstack/react-router"
import type { ErrorComponentProps } from "@tanstack/react-router"
import { TanStackRouterDevtoolsPanel } from "@tanstack/react-router-devtools"
import { TanStackDevtools } from "@tanstack/react-devtools"
import { SiteHeader } from "@/components/site-header"
import { SiteFooter } from "@/components/site-footer"

import appCss from "../styles.css?url"

export const Route = createRootRoute({
  head: () => ({
    meta: [
      {
        charSet: "utf-8",
      },
      {
        name: "viewport",
        content: "width=device-width, initial-scale=1",
      },
      {
        title: "DPRR — Digital Prosopography of the Roman Republic",
      },
      {
        name: "description",
        content:
          "Search and browse 4,876 persons from the Roman Republic (509–31 BC)",
      },
    ],
    links: [
      {
        rel: "stylesheet",
        href: appCss,
      },
    ],
  }),
  shellComponent: RootDocument,
  component: RootLayout,
  // Caught here (not in each leaf route) so SiteHeader/SiteFooter from
  // RootLayout keep rendering — only the Outlet's content is swapped out.
  errorComponent: RootError,
  notFoundComponent: RootNotFound,
})

function BackToSearch() {
  return (
    <Link to="/" className="text-accent-ink hover:underline">
      Back to search
    </Link>
  )
}

function RootError(_props: ErrorComponentProps) {
  return (
    <div className="mx-auto max-w-2xl px-4 py-16 text-center">
      <h1 className="font-heading text-2xl font-bold">
        Something went wrong loading this page.
      </h1>
      <p className="mt-4">
        <BackToSearch />
      </p>
    </div>
  )
}

function RootNotFound() {
  return (
    <div className="mx-auto max-w-2xl px-4 py-16 text-center">
      <h1 className="font-heading text-2xl font-bold">
        Not found — this record doesn't exist.
      </h1>
      <p className="mt-4">
        <BackToSearch />
      </p>
    </div>
  )
}

function RootLayout() {
  return (
    <>
      <SiteHeader />
      <Outlet />
      <SiteFooter />
    </>
  )
}

function RootDocument({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <head>
        <HeadContent />
      </head>
      <body>
        {children}
        {import.meta.env.DEV && (
          <TanStackDevtools
            config={{
              position: "bottom-right",
            }}
            plugins={[
              {
                name: "Tanstack Router",
                render: <TanStackRouterDevtoolsPanel />,
              },
            ]}
          />
        )}
        <Scripts />
      </body>
    </html>
  )
}
