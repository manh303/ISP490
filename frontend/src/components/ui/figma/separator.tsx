"use client";

import * as React from "react";
// Đã thay thế Radix UI Separator bằng separator thuần React

import { cn } from "./utils";

type SeparatorProps = {
  className?: string;
  orientation?: "horizontal" | "vertical";
  decorative?: boolean;
  [key: string]: any;
};

function Separator({
  className,
  orientation = "horizontal",
  decorative = true,
  ...props
}: SeparatorProps) {
  return orientation === "vertical" ? (
    <div
      role={decorative ? "presentation" : "separator"}
      className={cn(
        "bg-border w-px h-full shrink-0",
        className
      )}
      {...props}
    />
  ) : (
    <hr
      role={decorative ? "presentation" : "separator"}
      className={cn(
        "bg-border h-px w-full shrink-0 border-0",
        className
      )}
      {...props}
    />
  );
}

export { Separator };
