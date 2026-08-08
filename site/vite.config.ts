import { defineConfig } from "vite-plus"
import { devtools } from "@tanstack/devtools-vite"
import { tanstackStart } from "@tanstack/react-start/plugin/vite"
import viteReact from "@vitejs/plugin-react"
import tailwindcss from "@tailwindcss/vite"
import { staticDataPlugin } from "./src/build/static-data-plugin.ts"

const config = defineConfig({
  base: "/dprr-data/",
  // Vite resolves the `@/*` alias from tsconfig.json natively; the
  // vite-tsconfig-paths plugin is no longer needed.
  resolve: { tsconfigPaths: true },
  lint: { options: { typeAware: true, typeCheck: true } },
  fmt: {
    endOfLine: "lf",
    semi: false,
    singleQuote: false,
    tabWidth: 2,
    trailingComma: "es5",
    printWidth: 80,
    sortTailwindcss: {
      stylesheet: "src/styles.css",
      functions: ["cn", "cva"],
    },
    sortPackageJson: false,
    ignorePatterns: [
      "package-lock.json",
      "pnpm-lock.yaml",
      "yarn.lock",
      "src/routeTree.gen.ts",
    ],
  },
  plugins: [
    staticDataPlugin(),
    devtools(),
    tailwindcss(),
    tanstackStart({
      prerender: {
        enabled: true,
        crawlLinks: true,
      },
    }),
    viteReact(),
  ],
})

export default config
