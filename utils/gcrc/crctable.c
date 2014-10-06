/******************************************************************************/
/*                             Start of crctable.c                            */
/******************************************************************************/
/*                                                                            */
/* Author  : Ross Williams (ross@guest.adelaide.edu.au.).                     */
/* Date    : 3 June 1993.                                                     */
/* Version : 1.0.                                                             */
/* Status  : Public domain.                                                   */
/*                                                                            */
/* Description : This program writes a CRC lookup table (suitable for         */
/* inclusion in a C program) to a designated output file. The program can be  */
/* statically configured to produce any table covered by the Rocksoft^tm      */
/* Model CRC Algorithm. For more information on the Rocksoft^tm Model CRC     */
/* Algorithm, see the document titled "A Painless Guide to CRC Error          */
/* Detection Algorithms" by Ross Williams (ross@guest.adelaide.edu.au.). This */
/* document is likely to be in "ftp.adelaide.edu.au/pub/rocksoft".            */
/*                                                                            */
/* Note: Rocksoft is a trademark of Rocksoft Pty Ltd, Adelaide, Australia.    */
/*                                                                            */
/******************************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include "crcmodel.h"

/******************************************************************************/

/* TABLE PARAMETERS                                                           */
/* ================                                                           */
/* The following parameters entirely determine the table to be generated. You */
/* should need to modify only the definitions in this section before running  */
/* this program.                                                              */
/*                                                                            */
/*    TB_FILE  is the name of the output file.                                */
/*    TB_WIDTH is the table width in bytes (either 2 or 4).                   */
/*    TB_POLY  is the "polynomial", which must be TB_WIDTH bytes wide.        */
/*    TB_REVER indicates whether the table is to be reversed (reflected).     */
/*                                                                            */
/* Example:                                                                   */
/*                                                                            */
/*    #define TB_FILE   "crctable.out"                                        */
/*    #define TB_WIDTH  2                                                     */
/*    #define TB_POLY   0x8005L                                               */
/*    #define TB_REVER  TRUE                                                  */

#define TB_FILE   "crctable.out"
#define TB_WIDTH  4
//#define TB_POLY   0x04C11DB7L // CRC32
#define TB_POLY   0x1EDC6F41 // CRC32C (iSCSI), supported in SSE4.2 (since Nehalem)
#define TB_REVER  TRUE

/******************************************************************************/

/* Miscellaneous definitions. */

#define LOCAL static
FILE *outfile;
#define WR(X) fprintf(outfile,(X))
#define WP(X,Y) fprintf(outfile,(X),(Y))

/******************************************************************************/

LOCAL void chk_err P_((char *));
LOCAL void chk_err (mess)
/* If mess is non-empty, write it out and abort. Otherwise, check the error   */
/* status of outfile and abort if an error has occurred.                      */
char *mess;
{
 if (mess[0] != 0   ) {printf("%s\n",mess); exit(EXIT_FAILURE);}
 if (ferror(outfile)) {perror("chk_err");   exit(EXIT_FAILURE);}
}

/******************************************************************************/

LOCAL void chkparam P_((void));
LOCAL void chkparam ()
{
 if ((TB_WIDTH != 2) && (TB_WIDTH != 4))
    chk_err("chkparam: Width parameter is illegal.");
 if ((TB_WIDTH == 2) && (TB_POLY & 0xFFFF0000L))
    chk_err("chkparam: Poly parameter is too wide.");
 if ((TB_REVER != FALSE) && (TB_REVER != TRUE))
    chk_err("chkparam: Reverse parameter is not boolean.");
}

/******************************************************************************/

LOCAL void gentable P_((void));
LOCAL void gentable ()
{
 WR("/*****************************************************************/\n");
 WR("/*                                                               */\n");
 WR("/* CRC LOOKUP TABLE                                              */\n");
 WR("/* ================                                              */\n");
 WR("/* The following CRC lookup table was generated automagically    */\n");
 WR("/* by the Rocksoft^tm Model CRC Algorithm Table Generation       */\n");
 WR("/* Program V1.0 using the following model parameters:            */\n");
 WR("/*                                                               */\n");
 WP("/*    Width   : %1lu bytes.                                         */\n",
    (ulong) TB_WIDTH);
 if (TB_WIDTH == 2)
 WP("/*    Poly    : 0x%04lX                                           */\n",
    (ulong) TB_POLY);
 else
 WP("/*    Poly    : 0x%08lXL                                      */\n",
    (ulong) TB_POLY);
 if (TB_REVER)
 WR("/*    Reverse : TRUE.                                            */\n");
 else
 WR("/*    Reverse : FALSE.                                           */\n");
 WR("/*                                                               */\n");
 WR("/* For more information on the Rocksoft^tm Model CRC Algorithm,  */\n");
 WR("/* see the document titled \"A Painless Guide to CRC Error        */\n");
 WR("/* Detection Algorithms\" by Ross Williams                        */\n");
 WR("/* (ross@guest.adelaide.edu.au.). This document is likely to be  */\n");
 WR("/* in the FTP archive \"ftp.adelaide.edu.au/pub/rocksoft\".        */\n");
 WR("/*                                                               */\n");
 WR("/*****************************************************************/\n");
 WR("\n");
 switch (TB_WIDTH)
   {
    case 2: WR("unsigned short crctable[256] =\n{\n"); break;
    case 4: WR("unsigned long  crctable[256] =\n{\n"); break;
    default: chk_err("gentable: TB_WIDTH is invalid.");
   }
 chk_err("");

 {
  int i;
  cm_t cm;
  char *form    = (TB_WIDTH==2) ? "0x%04lX" : "0x%08lXL";
  int   perline = (TB_WIDTH==2) ? 8 : 4;

  cm.cm_width = TB_WIDTH*8;
  cm.cm_poly  = TB_POLY;
  cm.cm_refin = TB_REVER;

  for (i=0; i<256; i++)
    {
     WR(" ");
     WP(form,(ulong) cm_tab(&cm,i));
     if (i != 255) WR(",");
     if (((i+1) % perline) == 0) WR("\n");
     chk_err("");
    }

 WR("};\n");
 WR("\n");
 WR("/*****************************************************************/\n");
 WR("/*                   End of CRC Lookup Table                     */\n");
 WR("/*****************************************************************/\n");
 WR("");
 chk_err("");
}
}

/******************************************************************************/

int main ()
{
 printf("\n");
 printf("Rocksoft^tm Model CRC Algorithm Table Generation Program V1.0\n");
 printf("-------------------------------------------------------------\n");
 printf("Output file is \"%s\".\n",TB_FILE);
 chkparam();
 outfile = fopen(TB_FILE,"w"); chk_err("");
 gentable();
 if (fclose(outfile) != 0)
    chk_err("main: Couldn't close output file.");
 printf("\nSUCCESS: The table has been successfully written.\n");
 return 0;
}
