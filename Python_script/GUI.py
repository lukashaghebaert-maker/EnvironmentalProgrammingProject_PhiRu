# -*- coding: utf-8 -*-
"""
Created on Thu Dec 18 18:32:26 2025

@author: lukas
"""
import tkinter as tk
from tkinter import ttk, messagebox
from PIL import Image, ImageTk
import threading
import os
import sys


# Import your backend script
import WORKINGFILE_PhiRu_FUNCTION as backend

class AnalysisApp:
    def __init__(self, root):
        self.root = root
        self.root.title("EM-DAT vs Wikimpacts Analyzer")
        self.root.geometry("1000x800")
        
        # Store image paths here after analysis runs
        self.image_paths = {}

        # --- GUI LAYOUT ---
        
        # 1. Top Control Panel
        control_frame = ttk.Frame(root, padding=10)
        control_frame.pack(side="top", fill="x")

        # LableFrame to group settings visually
        settings_frame = ttk.LabelFrame(control_frame, text="Filter Settings", padding=10)
        settings_frame.pack(side="left", padx=10)

        # Start Year Input
        ttk.Label(settings_frame, text="Filter data after year:").pack(side="left")
        self.start_year_var = tk.StringVar(value="1900") # Default value
        self.entry_start = ttk.Entry(settings_frame, textvariable=self.start_year_var, width=8)
        self.entry_start.pack(side="left", padx=5) # Add some space to the right

        # The Run Button
        self.run_btn = ttk.Button(control_frame, text="Run Analysis", command=self.start_analysis_thread)
        self.run_btn.pack(side="left", padx=5)

        # Status Label (to show "Running..." or "Done")
        self.status_var = tk.StringVar(value="Ready")
        self.status_lbl = ttk.Label(control_frame, textvariable=self.status_var, font=("Arial", 10, "italic"))
        self.status_lbl.pack(side="left", padx=10)

        # 2. Graph Selection Buttons (Disabled initially)
        self.btn_frame = ttk.Frame(root, padding=10)
        self.btn_frame.pack(side="top", fill="x")
        
        self.view_deaths_btn = ttk.Button(self.btn_frame, text="View Deaths", state="disabled", command=lambda: self.show_image("Deaths"))
        self.view_deaths_btn.pack(side="left", padx=5)
        
        self.view_injuries_btn = ttk.Button(self.btn_frame, text="View Injuries", state="disabled", command=lambda: self.show_image("Injuries"))
        self.view_injuries_btn.pack(side="left", padx=5)
        
        self.view_damage_btn = ttk.Button(self.btn_frame, text="View Damage", state="disabled", command=lambda: self.show_image("Damage"))
        self.view_damage_btn.pack(side="left", padx=5)

        self.view_spatial_btn = ttk.Button(self.btn_frame, text="View Spatial Map", state="disabled",command=lambda: self.show_image("Spatial"))
        self.view_spatial_btn.pack(side="left", padx=5)

        # 3. Image Display Area
        self.image_canvas = tk.Label(root, text="Run analysis to generate graphs", bg="#f0f0f0")
        self.image_canvas.pack(side="top", fill="both", expand=True, padx=20, pady=20)
        
        # Rewire the "X" button as a safe exit
        self.root.protocol("WM_DELETE_WINDOW", self.safe_exit)

    def start_analysis_thread(self):
        """Starts the backend process in a separate thread so GUI doesn't freeze."""
        self.run_btn.config(state="disabled")
        self.status_var.set("Running analysis... Please wait.")
        
        # Check if the user typed a year
        try:
            s_year = int(self.start_year_var.get())
                
        except ValueError:
            messagebox.showerror("Input Error", "Please enter valid numbers for year.")
            return
        
        # Run the actual work in a background thread
        self.run_btn.config(state="disabled")
        self.status_var.set(f"Running analysis from {s_year} onwards ...")
        
        # Pass the years to the thread
        thread = threading.Thread(target=self.run_backend_logic, args=(s_year,))
        thread.start()

    def run_backend_logic(self,s_year):
        """This function runs in the background."""
        try:
            self.image_paths = backend.run_analysis(s_year)
            self.root.after(0, self.analysis_complete)
        except Exception as e:
            error_msg = str(e)
            self.root.after(0, lambda: messagebox.showerror("Error", error_msg))
            self.root.after(0, lambda: self.status_var.set("Error occurred."))
            self.root.after(0, lambda: self.run_btn.config(state="normal"))

    def analysis_complete(self):
        """Called when analysis finishes successfully."""
        self.status_var.set("Analysis Complete! Select a graph below.")
        self.run_btn.config(state="normal")
        
        # Enable the view buttons
        self.view_deaths_btn.config(state="normal")
        self.view_injuries_btn.config(state="normal")
        self.view_damage_btn.config(state="normal")
        self.view_spatial_btn.config(state="normal")
        
        
        
        # Show the first image automatically
        self.show_image("Deaths")

    def show_image(self, category):
        """Loads and displays the image for the selected category."""
        path = self.image_paths.get(category)
        if path and os.path.exists(path):
            # Load Image using Pillow
            load = Image.open(path)
            
            # Resize image to fit window if necessary (Optional)
            load = load.resize((800, 600), Image.Resampling.LANCZOS)
            
            render = ImageTk.PhotoImage(load)
            
            # Update the label
            self.image_canvas.config(image=render, text="")
            self.image_canvas.image = render # Keep a reference! (Crucial for Tkinter)
            self.status_var.set(f"Viewing: {category}")
        else:
            messagebox.showwarning("File Missing", f"Could not find image at {path}")
        
    def safe_exit(self):
        """Ensures the application and kernel shut down completely."""
        if messagebox.askokcancel("Quit", "Do you want to quit the program?"):
            # This kills the main window and stops the mainloop
            self.root.destroy()
            
            # Kills the python kernal
            sys.exit()

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    root = tk.Tk()
    app = AnalysisApp(root)
    root.mainloop()