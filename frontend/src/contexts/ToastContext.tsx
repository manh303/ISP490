import React, { createContext, useContext, useState, ReactNode } from 'react';
import Toast, { ToastProps } from '../components/common/Toast';

interface ToastContextType {
  showToast: (message: string, type: ToastProps['type'], duration?: number) => void;
}

const ToastContext = createContext<ToastContextType | undefined>(undefined);

interface ToastItem {
  id: string;
  message: string;
  type: ToastProps['type'];
  duration?: number;
}

interface ToastProviderProps {
  children: ReactNode;
}

export function ToastProvider({ children }: ToastProviderProps) {
  const [toasts, setToasts] = useState<ToastItem[]>([]);

  const showToast = (message: string, type: ToastProps['type'], duration?: number) => {
    const id = Math.random().toString(36).substr(2, 9);
    const newToast: ToastItem = {
      id,
      message,
      type,
      duration
    };

    setToasts(prev => [...prev, newToast]);
  };

  const removeToast = (id: string) => {
    setToasts(prev => prev.filter(toast => toast.id !== id));
  };

  return (
    <ToastContext.Provider value={{ showToast }}>
      {children}
      {/* Toast container with highest z-index and visible styling */}
      <div 
        className="fixed top-4 right-4 space-y-2" 
        style={{ 
          zIndex: 99999,
          pointerEvents: 'none',
          position: 'fixed',
          top: '1rem',
          right: '1rem'
        }}
      >
        {toasts.map(toast => (
          <div 
            key={toast.id} 
            style={{ pointerEvents: 'auto' }}
            className="transform transition-all duration-300"
          >
            <Toast
              message={toast.message}
              type={toast.type}
              duration={toast.duration}
              onClose={() => removeToast(toast.id)}
            />
          </div>
        ))}
      </div>
    </ToastContext.Provider>
  );
}

export function useToast(): ToastContextType {
  const context = useContext(ToastContext);
  if (context === undefined) {
    throw new Error('useToast must be used within a ToastProvider');
  }
  return context;
}