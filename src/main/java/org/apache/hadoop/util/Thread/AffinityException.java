/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.hadoop.util.Thread;

public class AffinityException extends RuntimeException {
        protected final Throwable rootCause;

        public AffinityException() {
                super();
                this.rootCause = null;
        }

        public AffinityException(final Throwable e) {
                super(getMsg(e), e);
                final Throwable root = getRootCause(e);
                this.setStackTrace(root.getStackTrace());
                this.rootCause = root == this ? null : root;
        }

        public AffinityException(final String msg) {
                super(msg);
                this.rootCause = null;
        }

        public AffinityException(final String msg, final Throwable e) {
                super(msg, e);
                final Throwable root = getRootCause(e);
                this.setStackTrace(root.getStackTrace());
                this.rootCause = root == this ? null : root;
        }

        private static String getMsg(final Throwable t) {
                final Throwable root = getRootCause(t);
                String msg = root.getMessage();
                if (msg == null || msg.length() == 0) {
                        msg = t.getMessage();
                        if (msg == null || msg.length() == 0) {
                                return root.getClass().getName();
                        }
                }
                return msg;
        }

        private static Throwable getRootCause(final Throwable t) {
                Throwable root = t.getCause();
                if (root == null) {
                        return t;
                }
                while (root.getCause() != null) {
                        root = root.getCause();
                }
                return root;
        }

        @Override
        public Throwable getCause() {
                return rootCause;
        }
}